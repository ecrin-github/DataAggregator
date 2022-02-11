using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
using System.Collections.Generic;
using Serilog;

namespace DataAggregator
{
    public class ObjectDataTransferrer
    {

        string _connString;
        DBUtilities db;
        ILogger _logger;

        public ObjectDataTransferrer(string connString, ILogger logger)
        {
            _connString = connString;
            _logger = logger;
            db = new DBUtilities(connString, _logger);
        }

        public void SetUpTempObjectIdsTables()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
                      CREATE TABLE IF NOT EXISTS nk.temp_object_ids(
                        object_id                INT
                      , source_id                INT
                      , sd_oid                   VARCHAR
                      , object_type_id		  	 INT            
                      , title                    VARCHAR      
                      , is_preferred_object      BOOLEAN
                      , parent_study_source_id   INT 
                      , parent_study_sd_sid      VARCHAR
                      , parent_study_id          INT
                      , is_preferred_study       BOOLEAN
                      , datetime_of_data_fetch   TIMESTAMPTZ
                      , match_status             INT   default 0
                      ); ";

                conn.Execute(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.temp_objects_to_add;
                      CREATE TABLE IF NOT EXISTS nk.temp_objects_to_add(
                        object_id                INT
                      , sd_oid                   VARCHAR
                      , object_type_id		  	 INT            
                      , title                    VARCHAR      
                      , is_preferred_object      BOOLEAN
                      ); 
                      CREATE INDEX temp_objects_to_add_sd_oid on nk.temp_objects_to_add(sd_oid);";

                conn.Execute(sql_string);
            }
        }


        public IEnumerable<ObjectId> FetchObjectIds(int source_id, string source_conn_string)
        {
            using (var conn = new NpgsqlConnection(source_conn_string))
            {
                string sql_string = @"select " + source_id.ToString() + @" as source_id, " 
                          + source_id.ToString() + @" as parent_study_source_id, 
                          sd_oid, object_type_id, title, sd_sid as parent_study_sd_sid, 
                          datetime_of_data_fetch
                          from ad.data_objects";

                return conn.Query<ObjectId>(sql_string);
            }
        }


        public ulong StoreObjectIds(PostgreSQLCopyHelper<ObjectId> copyHelper, IEnumerable<ObjectId> entities)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public void MatchExistingObjectIds()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                // Do these source - object id combinations already exist in the system,
                // i.e. have a known id. If they do they can be matched, to leave only 
                // the new object ids to process

                string sql_string = @"UPDATE nk.temp_object_ids t
                        SET object_id = doi.object_id, 
                        is_preferred_object = doi.is_preferred_object,
                        parent_study_id = doi.parent_study_id,
                        is_preferred_study = doi.is_preferred_study,
                        match_status = 1
                        from nk.data_object_identifiers doi
                        where t.source_id = doi.source_id
                        and t.sd_oid = doi.sd_oid";

                db.ExecuteSQL(sql_string);

                // also update the data_object_identifiers table
                // Indicates has been matched and updates the 
                // data fetch date

                sql_string = @"UPDATE nk.data_object_identifiers doi
                set match_status = 1,
                datetime_of_data_fetch = t.datetime_of_data_fetch
                from nk.temp_object_ids t
                where t.source_id = doi.source_id
                and t.sd_oid = doi.sd_oid;";

                int res = db.ExecuteSQL(sql_string);
                _logger.Information(res.ToString() + " existing objects found");
            }
        }


        public void UpdateNewObjectsWithStudyIds(int source_id)
        {
            // For the new objects...where match_status still 0
            
            // Update the object parent study_id using the 'correct'
            // value found in the study_identifiers table

            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"UPDATE nk.temp_object_ids t
                           SET parent_study_id = si.study_id, 
                           is_preferred_study = si.is_preferred_study
                           FROM nk.study_identifiers si
                           WHERE t.parent_study_sd_sid = si.sd_sid
                           and t.parent_study_source_id = si.source_id;";
                int res = db.ExecuteSQL(sql_string);
                _logger.Information(res.ToString() + " new objects found");

                // Drop those object records that cannot be matched
                // N.B. study linked records - Pubmed objects do not 
                // travel down this path

                sql_string = @"DELETE FROM nk.temp_object_ids
                             WHERE parent_study_id is null;";
                res = db.ExecuteSQL(sql_string);
                _logger.Information(res.ToString() + " objects dropped as no matching study found");
            }
        }


        public void UpdateAllObjectIdsTable(int source_id)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                // Add all the new object id records to the all Ids table

                string sql_string = @"INSERT INTO nk.data_object_identifiers
                             (source_id, sd_oid, object_type_id, title, is_preferred_object,
                              parent_study_source_id, parent_study_sd_sid,
                              parent_study_id, is_preferred_study, datetime_of_data_fetch)
                              select 
                              source_id, sd_oid, object_type_id, title, is_preferred_object,
                              parent_study_source_id, parent_study_sd_sid,
                              parent_study_id, is_preferred_study, datetime_of_data_fetch
                              from nk.temp_object_ids
                              where match_status = 0";
                conn.Execute(sql_string);
                int res = db.ExecuteSQL(sql_string);
                _logger.Information(res.ToString() + " new objects into object identifiers table");

                // For study based data, if the study is 'preferred' it is the first time
                // that it and related data objects can be added to the database, so
                // set the object id to the table id and set the match_status to 2

                sql_string = @"UPDATE nk.data_object_identifiers
                            SET object_id = id, is_preferred_object = true,
                            match_status = 2
                            WHERE object_id is null
                            AND source_id = " + source_id.ToString() + @"
                            AND is_preferred_study = true;";

                res = db.ExecuteSQL(sql_string);

                // For data objects from 'non-preferred' studies, there may be duplicate 
                // data objects already in the system, but that does not apply to registry
                // linked objects such as registry entries, results entries, web landing pages

                sql_string = @"UPDATE nk.data_object_identifiers
                            SET object_id = id, is_preferred_object = true,
                            match_status = 2
                            WHERE object_id is null
                            AND source_id = " + source_id.ToString() + @"
                            AND object_type_id in (13, 28);";
                res += db.ExecuteSQL(sql_string);

                if (source_id == 101900 || source_id == 101901)  // BioLINCC or Yoda
                {
                    sql_string = @"UPDATE nk.data_object_identifiers
                            SET object_id = id, is_preferred_object = true,
                            match_status = 2
                            WHERE object_id is null
                            AND source_id = " + source_id.ToString() + @"
                            AND object_type_id in (38);";
                    res += db.ExecuteSQL(sql_string);
                }

                _logger.Information(res.ToString() + " new objects specified as preferred, for addition");
            }
        }


        public void CheckNewObjectsForDuplicates(int source_id)
        {
            // Any more new object records to be checked

            // get the set of distinct studies that are linked to
            // object records that still have no object id
            // if there are any, continue...
            // by getting the instance URLs attached to the objects
            // linked to the 'preferred' version of the studies 
            // (which equates to thiose already in the system...)

            // and by getting the URLs linked back in the source database 
            // with these studies

            // if any match in the new set - they can be ignored
            // because they are duplicates - set match status to -1 


            
            
            // duplicates may be picked up from instance URLs
            // or by considering type and title.
            // In the second case may be a duplicate if the same URL

            // if not, and both URLs are present, may be a different instance
            // of the same object type / title
            // in that case the object id should be the original 'preferred'
            // object, but the object itself shou






        }


        public void FillObjectsToAddTable(int source_id)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"INSERT INTO nk.temp_objects_to_add
                             (object_id, sd_oid)
                             SELECT distinct object_id, sd_oid 
                             FROM nk.all_ids_data_objects
                             WHERE source_id = " + source_id.ToString();
                conn.Execute(sql_string);
            }
        }


        public int LoadDataObjects(string schema_name)
        {
             string sql_string = @"INSERT INTO ob.data_objects(id,
                    title, version, display_title, doi, doi_status_id, 
                    publication_year, object_class_id, object_type_id, 
                    managing_org_id, managing_org, managing_org_ror_id,
                    lang_code, access_type_id, access_details, access_details_url,
                    url_last_checked, eosc_category, add_study_contribs, 
                    add_study_topics)
                    SELECT t.object_id,
                    s.title, s.version, s.display_title, s.doi, s.doi_status_id, 
                    s.publication_year, s.object_class_id, s.object_type_id, 
                    s.managing_org_id, s.managing_org, s.managing_org_ror_id,
                    s.lang_code, s.access_type_id, s.access_details, s.access_details_url,
                    s.url_last_checked, s.eosc_category, s.add_study_contribs, 
                    s.add_study_topics
                    FROM " + schema_name + @".data_objects s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "data_objects", "");
            _logger.Information("Loaded records - " + res.ToString() + " data_objects");

            // db.Update_SourceTable_ExportDate(schema_name, "data_objects");
            return res;
        }

    
        public void LoadObjectDatasets(string schema_name)
        {
            string sql_string = @"INSERT INTO ob.object_datasets(object_id, 
            record_keys_type_id, record_keys_details, 
            deident_type_id, deident_direct, deident_hipaa,
            deident_dates, deident_nonarr, deident_kanon, deident_details,
            consent_type_id, consent_noncommercial, consent_geog_restrict,
            consent_research_type, consent_genetic_only, consent_no_methods, consent_details)
            SELECT t.object_id, 
            record_keys_type_id, record_keys_details, 
            deident_type_id, deident_direct, deident_hipaa,
            deident_dates, deident_nonarr, deident_kanon, deident_details,
            consent_type_id, consent_noncommercial, consent_geog_restrict,
            consent_research_type, consent_genetic_only, consent_no_methods, consent_details
            FROM " + schema_name + @".object_datasets s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_datasets", "");
            _logger.Information("Loaded records - " + res.ToString() + " object_datasets");

            // db.Update_SourceTable_ExportDate(schema_name, "object_datasets");
        }


        public void LoadObjectInstances(string schema_name)
        {
            string sql_string = @"INSERT INTO ob.object_instances(object_id,  
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, resource_comments)
            SELECT t.object_id,
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, resource_comments
            FROM " + schema_name + @".object_instances s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_instances", "");
            _logger.Information("Loaded records - " + res.ToString() + " object_instances");

            // db.Update_SourceTable_ExportDate(schema_name, "object_instances");
        }


        public void LoadObjectTitles(string schema_name)
        {
            string sql_string = @"INSERT INTO ob.object_titles(object_id, 
            title_type_id, title_text, lang_code,
            lang_usage_id, is_default, comments)
            SELECT t.object_id, 
            title_type_id, title_text, lang_code,
            lang_usage_id, is_default, comments
            FROM " + schema_name + @".object_titles s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_titles", "");
            _logger.Information("Loaded records - " + res.ToString() + " object_titles");

            // db.Update_SourceTable_ExportDate(schema_name, "object_titles");
        }


        public void LoadObjectDates(string schema_name)
        {
            string sql_string = @"INSERT INTO ob.object_dates(object_id, 
            date_type_id, date_is_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details)
            SELECT t.object_id,
            date_type_id, date_is_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details
            FROM " + schema_name + @".object_dates s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_dates", "");
            _logger.Information("Loaded records - " + res.ToString() + " object_dates");

            // db.Update_SourceTable_ExportDate(schema_name, "object_dates");
        }


        public void LoadObjectContributors(string schema_name)
        {
            string sql_string = @"INSERT INTO ob.object_contributors(object_id, 
            contrib_type_id, is_individual, 
            person_id, person_given_name, person_family_name, person_full_name,
            orcid_id, person_affiliation, organisation_id, 
            organisation_name, organisation_ror_id )
            SELECT t.object_id,
            contrib_type_id, is_individual, 
            person_id, person_given_name, person_family_name, person_full_name,
            orcid_id, person_affiliation, organisation_id, 
            organisation_name, organisation_ror_id 
            FROM " + schema_name + @".object_contributors s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_contributors", "");
            _logger.Information("Loaded records - " + res.ToString() + " object_contributors");

            // db.Update_SourceTable_ExportDate(schema_name, "object_contributors");
        }



        public void LoadObjectTopics(string schema_name)
        {
            string sql_string = @"INSERT INTO ob.object_topics(object_id, 
            topic_type_id, mesh_coded, mesh_code, mesh_value, 
            original_ct_id, original_ct_code, original_value)
            SELECT t.object_id, 
            topic_type_id, mesh_coded, mesh_code, mesh_value, 
            original_ct_id, original_ct_code, original_value
            FROM " + schema_name + @".object_topics s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_topics", "");
            _logger.Information("Loaded records - " + res.ToString() + " object_topics");

            // db.Update_SourceTable_ExportDate(schema_name, "object_topics");
        }


        public void LoadObjectDescriptions(string schema_name)
        {
            string sql_string = @"INSERT INTO ob.object_descriptions(object_id, 
            description_type_id, label, description_text, lang_code)
            SELECT t.object_id,
            description_type_id, label, description_text, lang_code
            FROM " + schema_name + @".object_descriptions s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_descriptions", "");
            _logger.Information("Loaded records - " + res.ToString() + " object_descriptions");

            // db.Update_SourceTable_ExportDate(schema_name, "object_descriptions");
        }


        public void LoadObjectIdentifiers(string schema_name)
        {
            string sql_string = @"INSERT INTO ob.object_identifiers(object_id,  
            identifier_value, identifier_type_id, 
            identifier_org_id, identifier_org, identifier_org_ror_id,
            identifier_date)
            SELECT t.object_id,
            identifier_value, identifier_type_id, 
            identifier_org_id, identifier_org, identifier_org_ror_id,
            identifier_date
            FROM " + schema_name + @".object_identifiers s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_identifiers", "");
            _logger.Information("Loaded records - " + res.ToString() + " object_identifiers");

            // db.Update_SourceTable_ExportDate(schema_name, "object_identifiers");
        }



        public void LoadObjectRelationships(string schema_name)
        {
            string sql_string = @"INSERT INTO ob.object_relationships(object_id,  
            relationship_type_id)
            SELECT t.object_id,
            relationship_type_id
            FROM " + schema_name + @".object_relationships s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            // NEED TO DO UPDATE OF TARGET SEPARATELY

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_relationships", "");
            _logger.Information("Loaded records - " + res.ToString() + " object_relationships");

            // db.Update_SourceTable_ExportDate(schema_name, "object_relationships");
        }



        public void LoadObjectRights(string schema_name)
        {
            string sql_string = @"INSERT INTO ob.object_rights(object_id,  
            rights_name, rights_uri, comments)
            SELECT t.object_id,
            rights_name, rights_uri, comments
            FROM " + schema_name + @".object_rights s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_rights", "");
            _logger.Information("Loaded records - " + res.ToString() + " object_rights");

            // db.Update_SourceTable_ExportDate(schema_name, "object_rights");
        }


        public void DropTempObjectIdsTable()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
                          DROP TABLE IF EXISTS nk.temp_objects_to_add;";
                conn.Execute(sql_string);
            }
        }

    }
}
