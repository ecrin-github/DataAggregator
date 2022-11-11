using System.Collections.Generic;


namespace DataAggregator
{
    class JSONObjectProcessor
    {
        JSONObjectDataLayer repo;
        LoggingHelper _loggingHelper;

        private DBDataObject ob;
        private lookup object_class;
        private lookup object_type;
        private organisation managing_organisation;
        private lookup access_type;
        private object_access access_details;
        private record_keys ds_record_keys;
        private deidentification ds_deident_level;
        private consent ds_consent;
       

        public JSONObjectProcessor(JSONObjectDataLayer _repo, LoggingHelper loggingHelper)
        {
            repo = _repo;
            _loggingHelper = loggingHelper;
        }

        public JSONDataObject CreateObject(int id)
        {
            // Re-initialise these compound properties.

            object_class = null;
            object_type = null;
            access_type = null;
            managing_organisation = null;
            access_details = null;
            ds_record_keys = null;
            ds_deident_level = null;
            ds_consent = null;

    
            // Get the singleton data object properties from DB

            ob = repo.FetchDbDataObject(id);

            // First check there is at least one linked study
            // (several hundred of the journal articles are not linked).

            var studies = new List<DBStudyObjectLink>(repo.FetchDbLinkedStudies(id));
            if (studies.Count == 0)
            {
                // May occur in a few hundred cases, therefore
                // if it does need to investigate further !!!!!!!
                // Possible (minor) error in data object linkage with journal articles.
                _loggingHelper.LogError("object " + ob.id + " does not appear to be linked to studies");
                return null;
            }

            // Instantiate the top level lookup types

            object_class = new lookup(ob.object_class_id, ob.object_class);
            object_type = new lookup(ob.object_type_id, ob.object_type);
            if (ob.managing_org != null)
            {
                managing_organisation = new organisation(ob.managing_org_id, ob.managing_org, ob.managing_org_ror_id);
            }
            if (ob.access_type_id != null)
            {
                access_type = new lookup(ob.access_type_id, ob.access_type);
            }
            if (ob.access_details != null || ob.access_details_url != null)
            {
                access_details = new object_access(ob.access_details, ob.access_details_url, ob.url_last_checked);
            }

            // Instantiate data object with those details

            JSONDataObject dobj = new JSONDataObject(ob.id, ob.doi, ob.display_title, ob.version, object_class,
                                  object_type, ob.publication_year, managing_organisation, ob.lang_code,
                                  access_type, access_details, ob.eosc_category, ob.provenance_string);


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

                dobj.dataset_record_keys = ds_record_keys;
                dobj.dataset_deident_level = ds_deident_level;
                dobj.dataset_consent = ds_consent;
            }


            dobj.object_instances = FetchObjectInstances(id);
            dobj.object_titles = FetchObjectTitles(id);
            dobj.object_dates = FetchObjectDates(id);
            dobj.object_contributors = FetchObjectContributors(id);
            dobj.object_topics = FetchObjectTopics(id);
            dobj.object_identifiers = FetchObjectIdentifiers(id);
            dobj.object_descriptions = FetchObjectDescriptions(id);
            dobj.object_rights = FetchObjectRights(id);
            dobj.object_relationships = FetchObjectRelationships(id);
            dobj.linked_studies = FetchLinkedStudies(id);


            return dobj;

 
        }


        private List<object_instance> FetchObjectInstances(int id)
        {
            List<object_instance> object_instances = null;

            var db_object_instances = new List<DBObjectInstance>(repo.FetchDbObjectInstances(id));
            if (db_object_instances.Count > 0)
            {
                object_instances = new List<object_instance>();
                foreach (DBObjectInstance i in db_object_instances)
                {
                    lookup repo_org = null;
                    access_details access = null;
                    resource_details resource = null;
                    if (i.repository_org != null)
                    {
                        repo_org = new lookup(i.repository_org_id, i.repository_org);
                    }
                    if (i.url != null || i.url_accessible != null)
                    {
                        access = new access_details(i.url, i.url_accessible, i.url_last_checked);
                    }
                    if (i.resource_type_id != null || i.comments != null)
                    {
                        resource = new resource_details(i.resource_type_id, i.resource_type,
                                                i.resource_size, i.resource_size_units, i.comments);
                    }
                    object_instances.Add(new object_instance(i.id, repo_org, access, resource));
                }
            }
            return object_instances;
        }


        private List<object_title> FetchObjectTitles(int id)
        {
            List<object_title> object_titles = null;

            var db_object_titles = new List<DBObjectTitle>(repo.FetchDbObjectTitles(id));
            if (db_object_titles.Count > 0)
            {
                object_titles = new List<object_title>();
                foreach (DBObjectTitle t in db_object_titles)
                {
                    object_titles.Add(new object_title(t.id, new lookup(t.title_type_id, t.title_type), t.title_text,
                                        t.lang_code, t.comments));
                }
            }
            return object_titles;
        }


        private List<object_date> FetchObjectDates(int id)
        {

            List<object_date> object_dates = null;

            var db_object_dates = new List<DBObjectDate>(repo.FetchDbObjectDates(id));
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
                    object_dates.Add(new object_date(d.id, new lookup(d.date_type_id, d.date_type), d.date_is_range,
                                                d.date_as_string, start_date, end_date, d.comments));
                }
            }
            return object_dates;
        }


        private List<object_contributor> FetchObjectContributors(int id)
        {
            List<object_contributor> object_contributors = null;

            // do individual contributors
              
            var db_object_contributors = new List<DBObjectContributor>(repo.FetchDbObjectContributors(id, "indiv"));
            if (db_object_contributors.Count > 0)
            {
                object_contributors = new List<object_contributor>();
                foreach (DBObjectContributor t in db_object_contributors)
                {
                    object_contributors.Add(new object_contributor(t.id,
                                     new lookup(t.contrib_type_id, t.contrib_type), true,
                                     new individual(t.person_family_name, t.person_given_name, t.person_full_name,
                                      t.orcid_id, t.person_affiliation, t.organisation_id, t.organisation_name,
                                      t.organisation_ror_id), null));
                }
            }

            // do organisational contributors

            db_object_contributors = new List<DBObjectContributor>(repo.FetchDbObjectContributors(id, "org"));
            if (db_object_contributors.Count > 0)
            {
                if (object_contributors == null)
                {
                    object_contributors = new List<object_contributor>();
                }
                foreach (DBObjectContributor t in db_object_contributors)
                {
                    object_contributors.Add(new object_contributor(t.id,
                                     new lookup(t.contrib_type_id, t.contrib_type), false, null,
                                     new organisation(t.organisation_id, t.organisation_name, t.organisation_ror_id)));
                }
            }
            return object_contributors;
        }


        private List<object_topic> FetchObjectTopics(int id)
        {
            List<object_topic> object_topics = null;
            // Get object topics -
            // source will depend on boolean flag, itself dependent on the type of object.

            var db_object_topics = new List<DBObjectTopic>(repo.FetchDbObjectTopics(id, ob.add_study_topics));
            if (db_object_topics.Count > 0)
            {
                object_topics = new List<object_topic>();
                foreach (DBObjectTopic t in db_object_topics)
                {
                    object_topics.Add(new object_topic(t.id, new lookup(t.topic_type_id, t.topic_type),
                                          t.mesh_coded, t.mesh_code, t.mesh_value,
                                          t.original_ct_id, t.original_ct_code, t.original_value));
                }
            }
            return object_topics;
        }


        private List<object_identifier> FetchObjectIdentifiers(int id)
        {
            List<object_identifier> object_identifiers = null;

            var db_object_identifiers = new List<DBObjectIdentifier>(repo.FetchDbObjectIdentifiers(id));
            if (db_object_identifiers.Count > 0)
            {
                object_identifiers = new List<object_identifier>();
                foreach (DBObjectIdentifier i in db_object_identifiers)
                {
                    object_identifiers.Add(new object_identifier(i.id, i.identifier_value,
                                          new lookup(i.identifier_type_id, i.identifier_type),
                                          new organisation(i.identifier_org_id, i.identifier_org, i.identifier_org_ror_id),
                                          i.identifier_date));
                }
            }
            return object_identifiers;
        }


        private List<object_description> FetchObjectDescriptions(int id)
        {
            List<object_description> object_descriptions = null;

            var db_object_descriptions = new List<DBObjectDescription>(repo.FetchDbObjectDescriptions(id));
            if (db_object_descriptions.Count > 0)
            {
                object_descriptions = new List<object_description>();
                foreach (DBObjectDescription i in db_object_descriptions)
                {
                    object_descriptions.Add(new object_description(i.id, new lookup(i.description_type_id, i.description_type),
                                         i.label, i.description_text, i.lang_code));
                }
            }
            return object_descriptions;
        }


        private List<object_right> FetchObjectRights(int id)
        {
            List<object_right> object_rights = null;

            var db_object_rights = new List<DBObjectRight>(repo.FetchDbObjectRights(id));
            if (db_object_rights.Count > 0)
            {
                object_rights = new List<object_right>();
                foreach (DBObjectRight i in db_object_rights)
                {
                    object_rights.Add(new object_right(i.id, i.rights_name, i.rights_uri, i.comments));
                }
            }
            return object_rights;
        }


        private List<object_relationship> FetchObjectRelationships(int id)
        {
            List<object_relationship> object_relationships = null;

            var db_object_relationships = new List<DBObjectRelationship>(repo.FetchDbObjectRelationships(id));
            if (db_object_relationships.Count > 0)
            {
                object_relationships = new List<object_relationship>();
                foreach (DBObjectRelationship i in db_object_relationships)
                {
                    object_relationships.Add(new object_relationship(i.id, new lookup(i.relationship_type_id, i.relationship_type),
                                                                     i.target_object_id));
                }
            }
            return object_relationships;
        }


        private List<int> FetchLinkedStudies(int id)
        {
            List<int> linked_studies = null;
            var db_study_object_links = new List<DBStudyObjectLink>(repo.FetchDbLinkedStudies(id));
            if (db_study_object_links.Count > 0)
            {
                linked_studies = new List<int>();
                foreach (DBStudyObjectLink t in db_study_object_links)
                {
                    linked_studies.Add(t.study_id);
                }
            }
            return linked_studies;
        }



        public void StoreJSONObjectInDB(int id, string object_json)
        {
            repo.StoreJSONObjectInDB(id, object_json); ;
        }

    }
}
