using System.Collections.Generic;

namespace DataAggregator
{
    public class JSONStudyProcessor
    {
        JSONStudyDataLayer repo;

        private lookup study_type;
        private lookup study_status;
        private lookup study_gender_elig;
        private age_param min_age;
        private age_param max_age;
        private year_month start_time;

        public JSONStudyProcessor(JSONStudyDataLayer _repo)
        {
            repo = _repo;
        }

        public JSONStudy CreateStudyObject(int id)
        {
            // Re-initialise these compound properties.
            study_type = null;
            study_status = null;
            study_gender_elig = null;
            min_age = null;
            max_age = null;
            start_time = null;

            // Get the singleton study properties from DB

            var s = repo.FetchDbStudy(id);

            // Instantiate the top level lookup types
            if (s.study_type_id != null)
            {
                study_type = new lookup(s.study_type_id, s.study_type);
            }
            if (s.study_status_id != null)
            {
                study_status = new lookup(s.study_status_id, s.study_status);
            }
            if (s.study_gender_elig_id != null)
            {
                study_gender_elig = new lookup(s.study_gender_elig_id, s.study_gender_elig);
            }
            if (s.min_age != null)
            {
                min_age = new age_param(s.min_age, s.min_age_units_id, s.min_age_units);
            }
            if (s.max_age != null)
            {
                max_age = new age_param(s.max_age, s.max_age_units_id, s.max_age_units);
            }
            if (s.study_start_year != null && s.study_start_month != null)
            {
                start_time = new year_month(s.study_start_year, s.study_start_month);
            }

            // instantiate a (json) study object and
            // fill it with study level details

            JSONStudy jst = new JSONStudy(s.id, s.display_title, s.brief_description,
                         s.data_sharing_statement, study_type, study_status, s.study_enrolment,
                         study_gender_elig, min_age, max_age, start_time, s.provenance_string);


            // get the 1-to-many properties and return the resulting 'json ready' study

            jst.study_identifiers = FetchStudyIdentifiers(id);
            jst.study_titles = FetchStudTitles(id);
            jst.study_features = FetchStudyFeatures(id);
            jst.study_topics = FetchStudyTopics(id);
            jst.study_contributors = FetchStudyContributors(id);
            jst.study_countries = FetchStudyCountries(id);
            jst.study_locations = FetchStudyLocations(id);
            jst.study_relationships = FetchStudyRelationships(id);
            jst.linked_data_objects = FetchLinkedObjects(id);

            return jst;
        }

        private List<study_identifier> FetchStudyIdentifiers(int id)
        {
            List<study_identifier> study_identifiers = null;
            var db_study_identifiers = new List<DBStudyIdentifier>(repo.FetchDbStudyIdentifiers(id));
            if (db_study_identifiers.Count > 0)
            {
                study_identifiers = new List<study_identifier>();
                foreach (DBStudyIdentifier t in db_study_identifiers)
                {
                    study_identifiers.Add(new study_identifier(t.id, t.identifier_value,
                                          new lookup(t.identifier_type_id, t.identifier_type),
                                          new organisation(t.identifier_org_id, t.identifier_org, t.identifier_org_ror_id),
                                          t.identifier_date, t.identifier_link));
                }
            }
            return study_identifiers;
        }


        private List<study_title> FetchStudTitles(int id)
        {
            List<study_title> study_titles = null;
            var db_study_titles = new List<DBStudyTitle>(repo.FetchDbStudyTitles(id));
            if (db_study_titles.Count > 0)
            {
                study_titles = new List<study_title>();
                foreach (DBStudyTitle t in db_study_titles)
                {
                    study_titles.Add(new study_title(t.id, new lookup(t.title_type_id, t.title_type),
                             t.title_text, t.lang_code, t.comments));
                }
            }
            return study_titles;
        }


        private List<study_topic> FetchStudyTopics(int id)
        {
            List<study_topic> study_topics = null;
            var db_study_topics = new List<DBStudyTopic>(repo.FetchDbStudyTopics(id));
            if (db_study_topics.Count > 0)
            {
                study_topics = new List<study_topic>();
                foreach (DBStudyTopic t in db_study_topics)
                {
                    study_topics.Add(new study_topic(t.id, new lookup(t.topic_type_id, t.topic_type),
                             t.mesh_coded, t.mesh_code, t.mesh_value,
                             t.original_ct_id, t.original_ct_code, t.original_value));
                }
            }
            return study_topics;
        }


        private List<study_feature> FetchStudyFeatures(int id)
        {
            List<study_feature> study_features = null;
            var db_study_features = new List<DBStudyFeature>(repo.FetchDbStudyFeatures(id));
            if (db_study_features.Count > 0)
            {
                study_features = new List<study_feature>();
                foreach (DBStudyFeature t in db_study_features)
                {
                    study_features.Add(new study_feature(t.id, new lookup(t.feature_type_id, t.feature_type),
                                            new lookup(t.feature_value_id, t.feature_value)));
                }
            }
            return study_features;
        }


        private List<study_contributor> FetchStudyContributors(int id)
        {
            List<study_contributor> study_contributors = null;

            // do individual contributors

            var db_study_contributors = new List<DBStudyContributor>(repo.FetchDbStudyContributors(id, "indiv"));
            if (db_study_contributors.Count > 0)
            {
                study_contributors = new List<study_contributor>();
                foreach (DBStudyContributor t in db_study_contributors)
                {
                    study_contributors.Add(new study_contributor(t.id,
                                     new lookup(t.contrib_type_id, t.contrib_type), true,
                                     new individual(t.person_family_name, t.person_given_name, t.person_full_name,
                                      t.orcid_id, t.person_affiliation, t.organisation_id, t.organisation_name,
                                      t.organisation_ror_id), null));
                }
            }

            // do organisational contributors

            db_study_contributors = new List<DBStudyContributor>(repo.FetchDbStudyContributors(id, "org"));
            if (db_study_contributors.Count > 0)
            {
                if (study_contributors == null)
                {
                    study_contributors = new List<study_contributor>();
                }
                foreach (DBStudyContributor t in db_study_contributors)
                {
                    study_contributors.Add(new study_contributor(t.id,
                                     new lookup(t.contrib_type_id, t.contrib_type), false, null,
                                     new organisation(t.organisation_id, t.organisation_name, t.organisation_ror_id)));
                }
            }
            return study_contributors;
        }


        private List<study_country> FetchStudyCountries(int id)
        {
            List<study_country> study_countries = null;
            var db_study_countries = new List<DBStudyCountry>(repo.FetchDbStudyCountries(id));
            if (db_study_countries.Count > 0)
            {
                study_countries = new List<study_country>();
                foreach (DBStudyCountry t in db_study_countries)
                {
                    study_countries.Add(new study_country(t.id,
                        new geonames_entity(t.country_id, t.country_name),
                        new lookup(t.status_id, t.status_type)));
                }
            }
            return study_countries;
        }


        private List<study_location> FetchStudyLocations(int id)
        {
            List<study_location> study_locations = null;
            var db_study_locations = new List<DBStudyLocation>(repo.FetchDbStudyLocations(id));
            if (db_study_locations.Count > 0)
            {
                study_locations = new List<study_location>();
                foreach (DBStudyLocation t in db_study_locations)
                {
                    study_locations.Add(new study_location(t.id,
                        new organisation(t.facility_org_id, t.facility, t.facility_ror_id),
                        new geonames_entity(t.city_id, t.city_name),
                        new geonames_entity(t.country_id, t.country_name),
                        new lookup(t.status_id, t.status_type)));
                }
            }
            return study_locations;
        }


        private List<study_relationship> FetchStudyRelationships(int id)
        {
            List<study_relationship> study_relationships = null;
            var db_study_relationships = new List<DBStudyRelationship>(repo.FetchDbStudyRelationships(id));
            if (db_study_relationships.Count > 0)
            {
                study_relationships = new List<study_relationship>();
                foreach (DBStudyRelationship t in db_study_relationships)
                {
                    study_relationships.Add(new study_relationship(t.id,
                              new lookup(t.relationship_type_id, t.relationship_type),
                              t.target_study_id));
                }
            }
            return study_relationships;
        }


        private List<int> FetchLinkedObjects(int id)
        {
            List<int> linked_data_objects = null;
            var db_study_object_links = new List<DBStudyObjectLink>(repo.FetchDbLinkedStudies(id));
            if (db_study_object_links.Count > 0)
            {
                linked_data_objects = new List<int>();
                foreach (DBStudyObjectLink t in db_study_object_links)
                {
                    linked_data_objects.Add(t.object_id);
                }
            }
            return linked_data_objects;
        }


        public void StoreJSONStudyInDB(int id, string study_json)
        {
            repo.StoreJSONStudyInDB(id, study_json); ;
        }

    }
}
