using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
	public class JSONDataLayer
	{
		private string connString;

        // These strings are used as the base of each query.
        // They are constructed once in the class constructor,
        // and can then be applied for each object constructed,
        // by adding the id parameter at the end of the string.

        private string study_query_string, study_identifier_query_string, study_title_query_string;
        private string study_object_link_query_string, study_relationship_query_string;
        private string study_feature_query_string, study_topics_query_string;

        private string data_object_query_string, data_set_query_string;
        private string object_link_query_string, object_identifier_query_string;
        private string object_date_query_string, object_title_query_string;
        private string object_contrib_query_string1, object_contrib_query_string2;
        private string object_study_contrib_query_string, object_study_topics_query_string;
        private string object_topics_query_string, object_instance_query_string;
        private string object_description_query_string, object_relationships_query_string;
        private string object_rights_query_string;

        public JSONDataLayer()
		{
				IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];
			builder.Database = "mdr";

			connString = builder.ConnectionString;

            ConstructStudyQueryStrings();
            ConstructObjectQueryStrings();

        }

		public string ConnString => connString;


		public int ExecuteSQL(string sql_string)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                try
                {
                    return conn.Execute(sql_string);
                }
                catch (Exception e)
                {
                    StringHelpers.SendError("In ExecuteSQL; " + e.Message + ", \nSQL was: " + sql_string);
                    return 0;
                }
            }
        }

        public int FetchMinId(string table_name)
        {
            string sql_string = @"select min(id) from core." + table_name;
            using (var conn = new NpgsqlConnection(connString))
            {
                return conn.ExecuteScalar<int>(sql_string);
            }
        }

        public int FetchMaxId(string table_name)
        {
            string sql_string = @"select max(id) from core." + table_name;
            using (var conn = new NpgsqlConnection(connString))
            {
                return conn.ExecuteScalar<int>(sql_string);
            }
        }

        public IEnumerable<int> FetchIds(string table_name, int n, int batch)
        {
            string sql_string = @"select id from core." + table_name + @"
                     where id between " + n.ToString() + @" 
                     and " + (n + batch - 1).ToString();
            using (var conn = new NpgsqlConnection(connString))
            {
                return conn.Query<int>(sql_string);
            }
        }


        private void ConstructObjectQueryStrings()
        {
            // data object query string
            data_object_query_string = @"Select dob.id, dob.doi, 
                dob.display_title, dob.version, 
                dob.object_class_id, oc.name as object_class,
                dob.object_type_id, ot.name as object_type,
                dob.publication_year, dob.lang_code, 
                dob.managing_org_id, dob.managing_org,
                dob.access_type_id, oat.name as access_type,
                dob.access_details, dob.access_details_url, dob.url_last_checked,
                dob.eosc_category, dob.add_study_contribs, dob.uses_study_topics,
                dob.provenance_string
                from core.data_objects dob
                left join context_lup.object_classes oc on dob.object_class_id = oc.id
                left join context_lup.object_types ot on dob.object_type_id = ot.id
                left join context_lup.object_access_types oat on dob.access_type_id = oat.id
                where dob.id = ";

            // dataset query string
            data_set_query_string = @"select ds.id, 
                ds.record_keys_type_id, rt.name as record_keys_type, 
                ds.record_keys_details,
                ds.deident_type_id, it.name as deident_type, 
                ds.deident_direct, ds.deident_hipaa, s.deident_dates,
                ds.deident_nonarr, ds.deident_kanon, ds.deident_details,
                ds.consent_type_id, ct.name as consent_type, 
                ds.consent_noncommercial, ds.consent_geog_restrict, ds.consent_research_type,
                ds.consent_genetic_only, ds.consent_no_methods, ds.consents_details
                from core.dataset_properties ds
                left join context_lup.dataset_record_key_types rt on ds.record_keys_type_id = rt.id
                left join context_lup.dataset_de-identification_levels it on ds.deident_type_id = it.id
                left join context_lup.dataset_consent_types ct on ds.consent_type_id = ct.id
                where ds.id = ";


            // object instances
            object_instance_query_string = @"select
                oi.id, instance_type_id, it.name as instance_type
                repository_org_id, repository_org, url,
                url_accessible, url_last_checked,
                resource_type_id, rt.name as resource_type,
                resource_size, resource_size_units, resource_comments as comments
                from core.object_instances oi
                left join context_lup.resource_types rt on oi.resource_type_id = rt.id
                left join context_lup.object_instance_types it on oi.instance_type_id = it.id
                where object_id = ";


            //object title query string
            object_title_query_string = @"select
                ot.id, ot.title_type_id, tt.name as title_type, 
                ot.title_text, ot.lang_code, ot.comments
                from core.object_titles ot
                left join context_lup.title_types tt on ot.title_type_id = tt.id
                where object_id = ";


            // object date query string
            object_date_query_string = @"select
                od.id, date_type_id, dt.name as date_type, is_date_range,
                date_as_string, start_year, start_month, start_day,
                end_year, end_month, end_day, details as comments
                from core.object_dates od
                left join context_lup.date_types dt on od.date_type_id = dt.id
                where object_id = ";

            
            // object contributor (using study contributors only)
            object_study_contrib_query_string = @"select
                sc.id, contrib_type_id, ct.name as contrib_type,
                is_individual, person_id, person_given_name, person_family_name,
                person_full_name, public_identifier, identifier_type,
                affiliation, affil_org_id, affil_org_id_type,
                organisation_id as contrib_org_id, organisation_name as contrib_org_name
                from core.study_object_links k
                inner join core.study_contributors sc on k.study_id = sc.study_id
                left join context_lup.contribution_types ct on sc.contrib_type_id = ct.id
                where object_id = ";


            // object contributor (using object contributors AND study organisations) - part 1
            object_contrib_query_string1 = @"select
                oc.id, contrib_type_id, ct.name as contrib_type,
                is_individual, organisation_id, organisation_name
                person_id, person_given_name, person_family_name,
                person_full_name, public_identifier, identifier_type,
                affiliation, affil_org_id, affil_org_id_type
                from core.object_contributors oc
                left join context_lup.contribution_types ct on oc.contrib_type_id = ct.id
                where object_id = ";


            // object contributor (using object contributors AND study organisations) - part 2
            object_contrib_query_string2 = @"select
                sc.id, contrib_type_id, ct.name as contrib_type,
                is_individual, organisation_id, organisation_name
                null, null, null, null, null, null, null, null, null
                from core.study_object_links k
                inner join core.study_contributors sc on k.study_id = sc.study_id
                left join context_lup.contribution_types ct on sc.contrib_type_id = ct.id
                where object_id = ";


            // object topics (using study objects)
            object_study_topics_query_string = @"select
                st.id, topic_type_id, tt.name as topic_type, mesh_coded
                topic_code, topic_value, topic_qualcode, topic_qualvalue
                original_value
                from core.study_object_links k
                inner join core.study_topics st on k.study_id = st.study_id
                left join context_lup.topic_types tt on st.topic_type_id = tt.id
                where k.object_id = ";


            // object topics (using object topics)
            object_topics_query_string = @"select
                ot.id, topic_type_id, tt.name as topic_type, mesh_coded
                topic_code, topic_value, topic_qualcode, topic_qualvalue
                original_value
                from core.object_topics ot
                left join context_lup.topic_types tt on ot.topic_type_id = tt.id
                where ot.object_id = ";


            // object identifiers query string 
            object_identifier_query_string = @"select
                oi.id, identifier_value, 
                identifier_type_id, it.name as identifier_type,
                identifier_org_id, identifier_org, identifier_date
                from core.object_identifiers oi
                left join context_lup.identifier_types it on oi.identifier_type_id = it.id
                where object_id = ";


            // object description query string 
            object_description_query_string = @"select
                id, description_type_id, dt.name as description_type
                label, description_text, lang_code, contains_html 
                from core.object_descriptions od
                left join context_lup.description_types dt
                on od.description_type_id = dt.id
                where object_id = ";


            // object relationships query string 
            object_relationships_query_string = @"select
                id, relationship_type_id, rt.name as relationship_type,
                target_object_id, from core.object_relationships
                from core.object_relationships or
                left join context_lup.object_relationship_types rt 
                on or.relationship_type_id = rt.id
                where object_id = ";


            // object rights query string 
            object_rights_query_string = @"select
                id, rights_name, rights_uri, comments
                from core.object_rights
                where object_id = ";


            // data study object link query string
            object_link_query_string = @"select study_id
                from core.study_object_links
                where object_id = ";
        }


        private void ConstructStudyQueryStrings()
        {
            // study query string
            study_query_string = @"Select s.id, display_title, title_lang_code,
                brief_description, bd_contains_html, 
                data_sharing_statement, dss_contains_html,
                study_type_id, st.name as study_type,
                study_status_id, ss.named as study_status
                study_enrolment, 
                study_gender_elig_id, ge.name as study_gender_elig, 
                min_age, min_age_units_id, tu1.name as min_age_units,
                max_age, max_age_units_id, tu2.name as max_age_units,
                provenance_string
                from core.studies s
                left join context_lup.study_types st on s.study_type_id = st.id
                left join context_lup.study_statuses ss on s.study_status_id = ss.id
                left join context_lup.gender_eligibilty_types ge on s.study_gender_elig_id = ge.id
                left join context_lup.time_units tu1 on s.min_age_units_id = tu1.id
                left join context_lup.time_units tu2 on s.min_age_units_id = tu2.id
                where s.id = ";


            // study identifier query string 
            study_identifier_query_string = @"select
                si.id, identifier_value,
                identifier_type_id, it.name as identifier_type,
                identifier_org_id, identifier_org,
                identifier_date, identifier_link
                from core.studyidentifiers si
                left join context_lup.identifier_types it on si.identifier_type_id = it.id
                where study_id = ";


            //study title query string
            study_title_query_string = @"select
                st.id, title_type_id, tt.name as title_type, title_text,
                lang_code, comments
                from core.study_titles st
                left join context_lup.title_types tt on st.title_type_id = tt.id
                where study_id = ";


            // study topics query string
            study_topics_query_string = @"select
                st.id, topic_type_id, tt.name as topic_type, mesh_coded
                topic_code, topic_value, topic_qualcode, topic_qualvalue
                original_value
                from core.study_topics st
                left join context_lup.topic_types tt on st.topic_type_id = tt.id
                where study_id = ";


            // study feature query string
            study_feature_query_string = @"select
                sf.id, feature_type_id, ft.name as feature_type,
                feature_value_id, fv.name as feature_value
                from core.study_features sf
                inner join context_lup.study_feature_types ft on sf.feature_type_id = ft.id
                left join context_lup.study_feature_categories fv on sc.contrib_type_id = fv.id
                where study_id = ";


            // study_relationship query string
            study_relationship_query_string = @"select
                sr.id, relationship_type_id, rt.name as relationship_type,
                target_study_id
                from core.study_relationships
                left join context_lup.study_relationship_types rt 
                on sr.relationship_type_id = rt.id
                where study_id = ";


            // study object link query string
            study_object_link_query_string = @"select object_id
                from core.study_object_links
                where study_id = ";

        }



        public DBStudy FetchDbStudy(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = study_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBStudy>(sql_string);
            }
        }


        public DBStudyIdentifier FetchDbStudyIdentifiers(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = study_identifier_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBStudyIdentifier>(sql_string);
            }
        }

        public DBStudyTitle FetchDbStudyTitles(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = study_title_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBStudyTitle>(sql_string);
            }
        }

        public DBStudyObjectLink FetchDbStudyObjectLinks(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = study_object_link_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBStudyObjectLink>(sql_string);
            }
        }

        public DBStudyRelationship FetchDbStudyRelationships(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = study_relationship_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBStudyRelationship>(sql_string);
            }
        }

        public DBStudyFeature FetchDbStudyFeatures(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = study_feature_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBStudyFeature>(sql_string);
            }
        }

        public DBStudyTopic FetchDbStudyTopics(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = study_topics_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBStudyTopic>(sql_string);
            }
        }


        // Fetches the main singleton data object attributes, used during the intiial 
        // construction of a data object by the Processor's CreateObject routine.

        public DBDataObject FetchDbDataObject(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = data_object_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBDataObject>(sql_string);
            }
        }


        // Fetches the data related to dataset properties, for 
        // data objects that are datasets.

        public DBDatasetProperties FetchDbDatasetProperties(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = data_set_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBDatasetProperties>(sql_string);
            }
        }


        // Fetches all linked instance records for the specified data object

        public IEnumerable<DBObjectInstance> FetchObjectInstances(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = object_instance_query_string + id.ToString();
                return Conn.Query<DBObjectInstance>(sql_string);
            }
        }


        // Fetches all linked study records for the specified data object

        public IEnumerable<int> FetchLinkedStudies(int Id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = object_link_query_string + Id.ToString();
                return Conn.Query<int>(sql_string);
            }
        }



        // Fetches all linked title records for the specified data object

        public IEnumerable<DBObjectTitle> FetchObjectTitles(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = object_title_query_string + id.ToString();
                return Conn.Query<DBObjectTitle>(sql_string);
            }
        }


        // Fetches all linked date for the specified data object

        public IEnumerable<DBObjectDate> FetchObjectDates(int Id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = object_date_query_string + Id.ToString();
                return Conn.Query<DBObjectDate>(sql_string);
            }
        }


        // Fetches all linked contributor records for the specified data object.
        // The boolean add_study_contribs, if true, indicates that the system should draw
        // the contributors from the corresponding 'parent' study's contributors
        // In these circumstances the object is assumed to have no linked contributors itself.
        // If false, the system draws the topics from the object's own contributor records, but 
        // it also unions these from any organisational contributors attached to the parent study 
        // (e.g. the sponsor).

        public IEnumerable<DBObjectContributor> FetchObjectContributors(int id, bool? add_study_contribs)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string;
                if ((bool)add_study_contribs)
                {
                    sql_string = object_study_contrib_query_string + id.ToString();
                }
                else
                {
                    sql_string = object_contrib_query_string1 + id.ToString() + " union select ";
                    sql_string += object_contrib_query_string2 + id.ToString() + " and is_individual = false";
                }
                return Conn.Query<DBObjectContributor>(sql_string);
            }
        }


        // Fetches all linked topic records for the specified data object.
        // The boolean use_study_topics, if true, indicates that the system should draw
        // the topics from the corresponding 'parent' study's topics.
        // In these circumstances the object is assumed to have no linked topics itself.
        // If false, the system draws the topics from the object's own topic records.

        public IEnumerable<DBObjectTopic> FetchObjectTopics(int id, bool? use_study_topics)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string;
                if ((bool)use_study_topics)
                {
                    sql_string = object_study_topics_query_string + id.ToString();
                }
                else
                {
                    sql_string = object_topics_query_string + id.ToString();
                }
                return Conn.Query<DBObjectTopic>(sql_string);
            }
        }

        // Fetches all linked identifier records for the specified data object

        public IEnumerable<DBObjectIdentifier> FetchObjectIdentifiers(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = object_identifier_query_string + id.ToString();
                return Conn.Query<DBObjectIdentifier>(sql_string);
            }
        }


        public IEnumerable<DBObjectDescription> FetchObjectDescriptions(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = object_description_query_string + id.ToString();
                return Conn.Query<DBObjectDescription>(sql_string);
            }
        }

        public IEnumerable<DBObjectRelationship> FetchObjectRelationships(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = object_relationships_query_string + id.ToString();
                return Conn.Query<DBObjectRelationship>(sql_string);
            }
        }


        public IEnumerable<DBObjectRight> FetchObjectRights(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = object_rights_query_string + id.ToString();
                return Conn.Query<DBObjectRight>(sql_string);
            }
        }


    }
}



