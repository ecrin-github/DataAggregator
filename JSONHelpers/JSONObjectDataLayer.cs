﻿using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataAggregator
{
    public class JSONObjectDataLayer
    {
        private string _connString;
        private string _object_json_folder;
        LoggingHelper _loggingHelper;

        // These strings are used as the base of each query.
        // They are constructed once in the class constructor,
        // and can then be applied for each object constructed,
        // by adding the id parameter at the end of the string.

        private string data_object_query_string, data_set_query_string;
        private string object_study_link_query_string, object_identifier_query_string;
        private string object_date_query_string, object_title_query_string;
        private string object_contrib_query_string, object_study_topics_query_string;
        private string object_topics_query_string, object_instance_query_string;
        private string object_description_query_string, object_relationships_query_string;
        private string object_rights_query_string;

        public JSONObjectDataLayer(LoggingHelper loggingHelper, string connString)
        {
            _loggingHelper = loggingHelper;
            _connString = connString;

            IConfigurationRoot settings = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            _object_json_folder = settings["object json folder"];

            ConstructObjectQueryStrings();

        }

        public string ConnString => _connString;
        public string ObjectJsonFolder => _object_json_folder;

        public int ExecuteSQL(string sql_string)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                try
                {
                    return conn.Execute(sql_string);
                }
                catch (Exception e)
                {
                    _loggingHelper.LogError("In ExecuteSQL; " + e.Message + ", \nSQL was: " + sql_string);
                    return 0;
                }
            }
        }

        public int FetchMinId()
        {
            string sql_string = @"select min(id) from core.data_objects";
            using (var conn = new NpgsqlConnection(_connString))
            {
                return conn.ExecuteScalar<int>(sql_string);
            }
        }

        public int FetchMaxId()
        {
            string sql_string = @"select max(id) from core.data_objects";
            using (var conn = new NpgsqlConnection(_connString))
            {
                return conn.ExecuteScalar<int>(sql_string);
            }
        }

        public IEnumerable<int> FetchIds(int n, int batch)
        {
            string sql_string = @"select id from core.data_objects
                     where id between " + n.ToString() + @" 
                     and " + (n + batch - 1).ToString();
            using (var conn = new NpgsqlConnection(_connString))
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
                dob.managing_org_id, dob.managing_org, dob.managing_org_ror_id,
                dob.access_type_id, oat.name as access_type,
                dob.access_details, dob.access_details_url, dob.url_last_checked,
                dob.eosc_category, dob.add_study_contribs, dob.add_study_topics,
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
                ds.deident_direct, ds.deident_hipaa, ds.deident_dates,
                ds.deident_nonarr, ds.deident_kanon, ds.deident_details,
                ds.consent_type_id, ct.name as consent_type, 
                ds.consent_noncommercial, ds.consent_geog_restrict, ds.consent_research_type,
                ds.consent_genetic_only, ds.consent_no_methods, ds.consent_details
                from core.object_datasets ds
                left join context_lup.dataset_recordkey_types rt on ds.record_keys_type_id = rt.id
                left join context_lup.dataset_deidentification_levels it on ds.deident_type_id = it.id
                left join context_lup.dataset_consent_types ct on ds.consent_type_id = ct.id
                where ds.id = ";


            // object instances
            object_instance_query_string = @"select
                oi.id, instance_type_id, it.name as instance_type,
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
                od.id, date_type_id, dt.name as date_type, date_is_range,
                date_as_string, start_year, start_month, start_day,
                end_year, end_month, end_day, details as comments
                from core.object_dates od
                left join context_lup.date_types dt on od.date_type_id = dt.id
                where object_id = ";

            // object contributor (using object contributors AND study organisations) - part 1
            object_contrib_query_string = @"select
                oc.id, contrib_type_id, ct.name as contrib_type, is_individual, 
                person_given_name, person_family_name, person_full_name,
                orcid_id, person_affiliation,
                organisation_id, organisation_name, organisation_ror_id
                from core.object_contributors oc
                left join context_lup.contribution_types ct on oc.contrib_type_id = ct.id
                where object_id = ";

            // object topics (using study objects)
            object_study_topics_query_string = @"select
                st.id, topic_type_id, tt.name as topic_type, 
                mesh_coded, mesh_code, mesh_value, 
                original_ct_id, original_ct_code, original_value
                from core.study_object_links k
                inner join core.study_topics st on k.study_id = st.study_id
                left join context_lup.topic_types tt on st.topic_type_id = tt.id
                where k.object_id = ";


            // object topics (using object topics)
            object_topics_query_string = @"select
                ot.id, topic_type_id, tt.name as topic_type, 
                mesh_coded, mesh_code, mesh_value, 
                original_ct_id, original_ct_code, original_value
                from core.object_topics ot
                left join context_lup.topic_types tt on ot.topic_type_id = tt.id
                where ot.object_id = ";


            // object identifiers query string 
            object_identifier_query_string = @"select
                oi.id, identifier_value, 
                identifier_type_id, it.name as identifier_type,
                identifier_org_id, identifier_org, 
                identifier_org_ror_id, identifier_date
                from core.object_identifiers oi
                left join context_lup.identifier_types it on oi.identifier_type_id = it.id
                where object_id = ";


            // object description query string 
            object_description_query_string = @"select
                od.id, description_type_id, dt.name as description_type,
                label, description_text, lang_code 
                from core.object_descriptions od
                left join context_lup.description_types dt
                on od.description_type_id = dt.id
                where object_id = ";


            // object relationships query string 
            object_relationships_query_string = @"select 
                r.id, relationship_type_id, rt.name as relationship_type,
                target_object_id 
                from core.object_relationships r
                left join context_lup.object_relationship_types rt 
                on r.relationship_type_id = rt.id
                where object_id = ";


            // object rights query string 
            object_rights_query_string = @"select
                id, rights_name, rights_uri, comments
                from core.object_rights
                where object_id = ";


            // data study object link query string
            object_study_link_query_string = @"select id,
                study_id, object_id
                from core.study_object_links
                where object_id = ";
            
        }


        // Fetches the main singleton data object attributes, used during the intiial 
        // construction of a data object by the Processor's CreateObject routine.

        public DBDataObject FetchDbDataObject(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = data_object_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBDataObject>(sql_string);
            }
        }


        // Fetches the data related to dataset properties, for 
        // data objects that are datasets.

        public DBDatasetProperties FetchDbDatasetProperties(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = data_set_query_string + id.ToString();
                return Conn.QueryFirstOrDefault<DBDatasetProperties>(sql_string);
            }
        }


        // Fetches all linked instance records for the specified data object

        public IEnumerable<DBObjectInstance> FetchDbObjectInstances(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = object_instance_query_string + id.ToString();
                return Conn.Query<DBObjectInstance>(sql_string);
            }
        }

                
        // Fetches all linked title records for the specified data object

        public IEnumerable<DBObjectTitle> FetchDbObjectTitles(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = object_title_query_string + id.ToString();
                return Conn.Query<DBObjectTitle>(sql_string);
            }
        }


        // Fetches all linked dates for the specified data object

        public IEnumerable<DBObjectDate> FetchDbObjectDates(int Id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = object_date_query_string + Id.ToString();
                return Conn.Query<DBObjectDate>(sql_string);
            }
        }


        // Fetches all contributors for the specified data object

        public IEnumerable<DBObjectContributor> FetchDbObjectContributors(int id, string contrib_type)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = object_contrib_query_string + id.ToString();
                if (contrib_type == "indiv")
                {
                    sql_string += " and is_individual = true";
                }
                else
                {
                    sql_string += " and is_individual = false";
                }
                return Conn.Query<DBObjectContributor>(sql_string);
            }
        }


        // Fetches all linked topic records for the specified data object.
        // The boolean use_study_topics, if true, indicates that the system should draw
        // the topics from the corresponding 'parent' study's topics.
        // In these circumstances the object is assumed to have no linked topics itself.
        // If false, the system draws the topics from the object's own topic records.

        public IEnumerable<DBObjectTopic> FetchDbObjectTopics(int id, bool? use_study_topics)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
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

        public IEnumerable<DBObjectIdentifier> FetchDbObjectIdentifiers(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = object_identifier_query_string + id.ToString();
                return Conn.Query<DBObjectIdentifier>(sql_string);
            }
        }


        public IEnumerable<DBObjectDescription> FetchDbObjectDescriptions(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = object_description_query_string + id.ToString();
                return Conn.Query<DBObjectDescription>(sql_string);
            }
        }

        public IEnumerable<DBObjectRelationship> FetchDbObjectRelationships(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = object_relationships_query_string + id.ToString();
                return Conn.Query<DBObjectRelationship>(sql_string);
            }
        }


        public IEnumerable<DBObjectRight> FetchDbObjectRights(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = object_rights_query_string + id.ToString();
                return Conn.Query<DBObjectRight>(sql_string);
            }
        }


        // Fetches all linked study records for the specified data object

        public IEnumerable<DBStudyObjectLink> FetchDbLinkedStudies(int Id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = object_study_link_query_string + Id.ToString();
                return Conn.Query<DBStudyObjectLink>(sql_string);
            }
        }


        public IEnumerable<int> FetchOAObjectIds()
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = "select object_id from nk.temp_oa_objects";
                return Conn.Query<int>(sql_string);
            }
        }


        public string FetchObjectJson(int id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                string sql_string = "select json from core.objects_json where id = " + id.ToString();
                return Conn.Query<string>(sql_string).FirstOrDefault();
            }
        }


        public void StoreJSONObjectInDB(int id, string object_json)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(_connString))
            {
                Conn.Open();

                // To insert the string into a json field the parameters for the 
                // command have to be explictly declared and typed

                using (var cmd = new NpgsqlCommand())
                {
                    cmd.CommandText = "INSERT INTO core.objects_json (id, json) VALUES (@id, @p)";
                    cmd.Parameters.Add(new NpgsqlParameter("@id", NpgsqlDbType.Integer) { Value = id });
                    cmd.Parameters.Add(new NpgsqlParameter("@p", NpgsqlDbType.Json) { Value = object_json });
                    cmd.Connection = Conn;
                    cmd.ExecuteNonQuery();
                }
                Conn.Close();
            }
        }

    }
}



