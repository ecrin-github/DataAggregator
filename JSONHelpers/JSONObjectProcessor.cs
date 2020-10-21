using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{ 
    class JSONObjectProcessor
    {
        JSONDataLayer repo;

        private DBDataObject ob;
        private lookup object_class;
        private lookup object_type;
        private lookup access_type;
        private lookup managing_org;
        private access_details access;
        private record_keys ds_record_keys;
        private deidentification ds_deident_level;
        private consent ds_consent;
       
        private List<object_identifier> object_identifiers;
        private List<object_title> object_titles;
        private List<object_contributor> object_contributors;
        private List<object_date> object_dates;
        private List<object_instance> object_instances;
        private List<object_topic> object_topics;
        private List<object_description> object_descriptions;
        private List<object_right> object_rights;
        private List<object_relationship> object_relationships;
        private List<int> linked_studies;

        public JSONObjectProcessor(JSONDataLayer _repo)
        {
            repo = _repo;
        }

        public DataObject CreateObject(int id)
        {
            // Re-initialise these compound properties.

            object_class = null;
            object_type = null;
            access_type = null;
            managing_org = null;
            access = null;
            ds_record_keys = null;
            ds_deident_level = null;
            ds_consent = null;

            object_titles = null; 
            object_contributors = null;
            object_dates = null; 
            object_instances = null;
            object_topics = null;
            object_identifiers = null;
            object_descriptions = null;
            object_rights = null;
            object_relationships = null;
            linked_studies = null;

            // Get the singleton data object properties from DB

            ob = repo.FetchDbDataObject(id);

            if (ob == null)
            {
                // Odd problem - should not occur but just in case...
                return null;
            }

            // First check there is at least one linked study
            // (several hundred of the journal articles are not linked).

            linked_studies = new List<int>(repo.FetchLinkedStudies(id));
            if (linked_studies.Count == 0)
            {
                // May occur in a few hundred cases, therefore
                // need to investigate further !!!!!!!
                // Possible (minor) error in data object linkage with journal articles.
                return null;
            }

            // Instantiate the top level lookup types

            object_class = new lookup(ob.object_class_id, ob.object_class);
            object_type = new lookup(ob.object_type_id, ob.object_type);
            access_type = new lookup(ob.access_type_id, ob.access_type);
            managing_org = new lookup(ob.managing_org_id, ob.managing_org);
            access = new access_details(ob.access_details, ob.access_details_url, ob.url_last_checked);

            // Instantiate data object with those details

            DataObject dobj = new DataObject(ob.id, ob.doi, ob.display_title,
                                object_class, object_type, ob.publication_year, managing_org, access_type,
                                access);


            // Get dataset properties, if there are any...

            var db_ds = repo.FetchDbDatasetProperties(id);
            if (db_ds != null)
            {
                ds_record_keys = new record_keys(db_ds.record_keys_type_id, db_ds.record_keys_type, db_ds.record_keys_details);
                ds_deident_level = new deidentification(db_ds.deident_type_id, db_ds.deident_type, db_ds.deident_direct,
                                             db_ds.deident_hipaa, db_ds.deident_dates, db_ds.deident_nonarr, 
                                             db_ds.deident_kanon, db_ds.deident_details);
                ds_consent = new consent(db_ds.consent_type_id, db_ds.consent_type, db_ds.consent_noncommercial,
                                             db_ds.consent_geog_restrict, db_ds.consent_research_type, db_ds.consent_genetic_only,
                                             db_ds.consent_no_methods, db_ds.consent_details);
            }

            // Get object identifiers.

            var db_object_identifiers = new List<DBObjectIdentifier>(repo.FetchObjectIdentifiers(id));
            if (db_object_identifiers.Count > 0)
            {
                object_identifiers = new List<object_identifier>();
                foreach (DBObjectIdentifier i in db_object_identifiers)
                {
                    object_identifiers.Add(new object_identifier(i.id, new lookup(i.identifier_type_id, i.identifier_type),
                                          new lookup(i.identifier_org_id, i.identifier_org), i.identifier_value, i.identifier_date));
                }
            }


            // Get object titles.

            var db_object_titles = new List<DBObjectTitle>(repo.FetchObjectTitles(id));
            if (db_object_titles.Count > 0)
            {
                object_titles = new List<object_title>();
                foreach (DBObjectTitle t in db_object_titles)
                {
                    object_titles.Add(new object_title(t.id, new lookup(t.title_type_id, t.title_type), t.title_text,
                                        t.lang_code, t.comments));
                }
            }



            // Get object dates.

            var db_object_dates = new List<DBObjectDate>(repo.FetchObjectDates(id));
            if (db_object_dates.Count > 0)
            {
                object_dates = new List<object_date>();
                sdate_as_ints start_date = null;
                edate_as_ints end_date = null;
                foreach (DBObjectDate d in db_object_dates)
                {
                    if (d.start_year != null || d.start_month != null || d.start_day != null)
                    {
                        start_date = new sdate_as_ints(d.start_year, d.start_month, d.start_day);
                    }
                    if (d.end_year != null || d.end_month != null || d.end_day != null)
                    {
                        end_date = new edate_as_ints(d.end_year, d.end_month, d.end_day);
                    }
                    object_dates.Add(new object_date(d.id, new lookup(d.date_type_id, d.date_type), d.is_date_range,
                                                d.date_as_string, start_date, end_date, d.comments));
                }
            }


            // Get object contributors - 
            // source will depend on boolean flag, itself dependent on the type of object.

            var db_object_contributors = new List<DBObjectContributor>(repo.FetchObjectContributors(id,  ob.add_study_contribs));
            if (db_object_contributors.Count > 0)
            {
                individual person; lookup org;
                object_contributors = new List<object_contributor>();

                foreach (DBObjectContributor c in db_object_contributors)
                {
                    person = null; org = null;
                    if (c.is_individual)
                    {
                        person = new individual(c.person_family_name, c.person_given_name, c.person_full_name,
                                                c.public_identifier, c.affiliation);
                    }
                    else
                    {
                        org = new lookup(c.organisation_id, c.organisation_name);
                    }
                    object_contributors.Add(new object_contributor(c.id, new lookup(c.contrib_type_id, c.contrib_type),
                                                c.is_individual, person, org));
                }
            }


            // Get object topics - 
            // source will depend on boolean flag, itself dependent on the type of object.

            var db_object_topics = new List<DBObjectTopic>(repo.FetchObjectTopics(id,  ob.add_study_topics));
            if (db_object_topics.Count > 0)
            {
                object_topics = new List<object_topic>();
                foreach (DBObjectTopic t in db_object_topics)
                {
                    object_topics.Add(new object_topic(t.id, new lookup(t.topic_type_id, t.topic_type), 
                                          t.mesh_coded, t.topic_code, t.topic_value, t.topic_qualcode,
                                          t.topic_qualvalue, t.original_value));
                }
            }


            // Get object instances.

            var db_object_instances = new List<DBObjectInstance>(repo.FetchObjectInstances(id));
            if (db_object_instances.Count > 0)
            {
                object_instances = new List<object_instance>();
                foreach (DBObjectInstance i in db_object_instances)
                {
                    object_instances.Add(new object_instance(i.id, new lookup(i.repository_org_id, i.repository_org),
                                                            i.url, i.url_accessible, i.url_last_checked,
                                                            new lookup(i.resource_type_id, i.resource_type),
                                                            i.resource_size, i.resource_size_units));
                }
            }


            // Construct the final data object by setting the composite 
            // and repreated properties to the classess and List<>s created above.

            dobj.dataset_consent = ds_consent;
            dobj.dataset_record_keys = ds_record_keys;
            dobj.dataset_deident_level = ds_deident_level;

            dobj.object_identifiers = object_identifiers;
            dobj.object_titles = object_titles;
            dobj.object_contributors = object_contributors;
            dobj.object_dates = object_dates;
            dobj.object_instances = object_instances;
            dobj.object_descriptions = object_descriptions;
            dobj.object_rights = object_rights;
            dobj.object_topics = object_topics;
            dobj.object_relationships = object_relationships;
            dobj.linked_studies = linked_studies;

            return dobj;
        }
    }
}
