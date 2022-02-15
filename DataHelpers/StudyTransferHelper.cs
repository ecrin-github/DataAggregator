using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
using System.Collections.Generic;
using Serilog;

namespace DataAggregator
{
    public class StudyDataTransferrer
    {
        string _connString;
        DBUtilities db;
        ILogger _logger;
        int nonpref_number;

        public StudyDataTransferrer(string connString, ILogger logger)
        {
            _connString = connString;
            _logger = logger;
            db = new DBUtilities(connString, _logger);
        }


        public void SetUpTempStudyIdsTable()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_ids;
                        CREATE TABLE nk.temp_study_ids(
                        id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                      , study_id                 INT
                      , source_id                INT
                      , sd_sid                   VARCHAR
                      , is_preferred             BOOLEAN
                      , datetime_of_data_fetch   TIMESTAMPTZ
                      , match_status             INT  default 0
                      ); ";
                conn.Execute(sql_string);
            }
        }

        public IEnumerable<StudyId> FetchStudyIds(int source_id, string source_conn_string)
        {
            using (var conn = new NpgsqlConnection(source_conn_string))
            {
                string sql_string = @"select " + source_id.ToString() + @" as source_id, 
                          sd_sid, datetime_of_data_fetch
                          from ad.studies
                          order by sd_sid";

                return conn.Query<StudyId>(sql_string);
            }
        }

        public ulong StoreStudyIds(PostgreSQLCopyHelper<StudyId> copyHelper, IEnumerable<StudyId> entities)
        {
            // stores the study id data in a temporary table
            using (var conn = new NpgsqlConnection(_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public void MatchExistingStudyIds()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                // Do these source - study id combinations already exist in the system,
                // i.e. have a known id. If they do they can be matched, to leave only 
                // the new study ids to process

                string sql_string = @"UPDATE nk.temp_study_ids t
                        SET study_id = si.study_id, is_preferred = si.is_preferred,
                        match_status = 1
                        from nk.study_identifiers si
                        where t.source_id = si.source_id
                        and t.sd_sid = si.sd_sid ";

                int res = db.Update_UsingTempTable("nk.temp_study_ids", "nk.temp_study_ids", sql_string);
                _logger.Information(res.ToString() + " existing studies matched in temp table");

                // also update the study_identifiers table
                // Indicates has been matched and updates the 
                // data fetch date

                sql_string = @"UPDATE nk.study_identifiers si
                set match_status = 1,
                datetime_of_data_fetch = t.datetime_of_data_fetch
                from nk.temp_study_ids t
                where t.source_id = si.source_id
                and t.sd_sid = si.sd_sid ";

                res = db.Update_UsingTempTable("nk.temp_study_ids", "nk.study_identifiers", sql_string);
                _logger.Information(res.ToString() + " existing studies matched in identifiers table");
            }
        }


        public void IdentifyNewLinkedStudyIds()
        {
            // For the new studies...where match_status still 0

            // Does any study id correspond to a study already in study_identifiers
            // table, that is linked to it via study-study link table.
            // Such a study will match the left hand side of the study-study 
            // link table (the one to be replaced), and take on the study_id 
            // used for the 'preferred' right hand side. This should already exist
            // because addition of studies is done in the order 'more preferred first'.

            string sql_string = @"UPDATE nk.temp_study_ids t
                           SET study_id = si.study_id, is_preferred = false,
                           match_status = 2
                           FROM nk.study_study_links k
                                INNER JOIN nk.study_identifiers si
                                ON k.preferred_sd_sid = si.sd_sid
                                AND k.preferred_source_id = si.source_id
                           WHERE t.sd_sid = k.sd_sid
                           AND t.source_id =  k.source_id
                           AND t.match_status = 0;";

            int res = db.ExecuteSQL(sql_string);
            _logger.Information(res.ToString() + " existing studies found under other study source ids");

        }

        public void AddNewStudyIds(int source_id)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                // For the new studies...where match_status still 0

                // Add all the new study id records to the all Ids table
                // This includes those identified above (match_status = 2)
                // and those yet to be added to the system (match_status = 0)

                string sql_string = @"INSERT INTO nk.study_identifiers
                            (study_id, source_id, sd_sid, 
                             datetime_of_data_fetch, is_preferred,
                             match_status)
                             select study_id, source_id, sd_sid, 
                             datetime_of_data_fetch, is_preferred, 
                             match_status
                             from nk.temp_study_ids t
                             where (match_status = 0 or match_status = 2) ";

                int res = db.Update_UsingTempTable("nk.temp_study_ids", "nk.study_identifiers", sql_string);
                _logger.Information(res.ToString() + " new study ids found");

                // Where the study_ids are null they can take on the value of the 
                // record id. The 3 indicates they are new on this addition.

                sql_string = @"UPDATE nk.study_identifiers
                            SET study_id = id, is_preferred = true,
                            match_status = 3
                            WHERE study_id is null
                            AND source_id = " + source_id.ToString();

                conn.Execute(sql_string);

                // 'Back-update' the study temp table using the newly created study_ids
                // now all records in this table should have a match and preferrd status

                sql_string = @"UPDATE nk.temp_study_ids t
                           SET study_id = si.study_id, is_preferred = true,
                           match_status = 3
                           FROM nk.study_identifiers si
                           WHERE t.source_id = si.source_id
                           AND t.sd_sid = si.sd_sid
                           AND t.study_id is null;";

                conn.Execute(sql_string);
            }
        }


        public void CreateTempStudyIdTables(int source_id)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                // Create two tables that has just the study_ids and sd_sids for the 
                // 'preferred' (new) studies (used to import all the linked data for these studies),
                // and the non-preferred (existing) studies (used in the import any additional data
                // from these studies)

                string sql_string = @"DROP TABLE IF EXISTS nk.new_studies;
                               CREATE TABLE nk.new_studies as 
                                       SELECT sd_sid, study_id
                                       FROM nk.study_identifiers
                                       WHERE source_id = " + source_id.ToString() + @" 
                                       and is_preferred = true";

                db.ExecuteSQL(sql_string);
                int res = db.GetCount("nk.new_studies");

                _logger.Information(res.ToString() + " preferred (full data) studies found");

                sql_string = @"DROP TABLE IF EXISTS nk.existing_studies;
                               CREATE TABLE nk.existing_studies as 
                                       SELECT sd_sid, study_id
                                       FROM nk.study_identifiers
                                       WHERE source_id = " + source_id.ToString() + @" 
                                       and is_preferred = false";

                db.ExecuteSQL(sql_string);
                res = db.GetCount("nk.existing_studies");
                _logger.Information(res.ToString() + " non-preferred (additional data) studies found");
                nonpref_number = res;
            }
        }

        public int LoadStudies(string schema_name)
        {
            _logger.Information("");

            // Insert the study data unless it is already in under another 
            // id (i.e. t.is_preferred = false).

            string sql_string = @"INSERT INTO st.studies(id, 
                    display_title, title_lang_code, brief_description, data_sharing_statement, 
                    study_start_year, study_start_month, study_type_id, study_status_id,
                    study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                    max_age, max_age_units_id)
                    SELECT t.study_id,
                    s.display_title, s.title_lang_code, s.brief_description, s.data_sharing_statement, 
                    s.study_start_year, s.study_start_month, s.study_type_id, s.study_status_id,
                    s.study_enrolment, s.study_gender_elig_id, s.min_age, s.min_age_units_id,
                    s.max_age, s.max_age_units_id
                    FROM nk.new_studies t
                    INNER JOIN " + schema_name + @".studies s
                    on t.sd_sid = s.sd_sid";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "studies", "new studies");
            _logger.Information("Transferred " + res.ToString() + " study data records");
            return res;
        }


        public void LoadStudyIdentifiers(string schema_name)
        {
            _logger.Information("");

            string destination_field_list = @"study_id, 
                    identifier_type_id, identifier_org_id, 
                    identifier_org, identifier_org_ror_id,
                    identifier_value, identifier_date, identifier_link ";

            string source_field_list = @" 
                    s.identifier_type_id, s.identifier_org_id, 
                    s.identifier_org, s.identifier_org_ror_id,
                    s.identifier_value, s.identifier_date, s.identifier_link ";

            // For 'preferred' study Ids add all identifiers.

            string sql_string = @"INSERT INTO st.study_identifiers(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.new_studies k
                    INNER JOIN " + schema_name + @".study_identifiers s
                    on k.sd_sid = s.sd_sid";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_identifiers", "new studies");
            _logger.Information("Transferred " + res.ToString() + " study identifiers, from 'preferred' studies");

            // For 'existing studies' study Ids add only new identifiers.

            if (nonpref_number > 0)
            {
                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_identifiers d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid";
                db.ExecuteSQL(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.identifier_type_id, c.identifier_value 
                           FROM st.study_identifiers c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id;";
                db.ExecuteSQL(sql_string);

                sql_string = @"INSERT INTO st.study_identifiers(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.identifier_type_id = e.identifier_type_id
                           AND s.identifier_value = e.identifier_value
                           WHERE e.study_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "study_identifiers", "existing studies");
                _logger.Information("Transferred " + res.ToString() + " study identifiers, from 'non-preferred' studies");
            }
        }


        public void LoadStudyTitles(string schema_name)
        {
            _logger.Information("");

            string destination_field_list = @"study_id, 
                    title_type_id, title_text, lang_code, 
                    lang_usage_id, is_default, comments ";

            string source_field_list = @" 
                    s.title_type_id, s.title_text, s.lang_code, 
                    s.lang_usage_id, s.is_default, s.comments ";

            // For 'preferred' study Ids add all titles.

            string sql_string = @"INSERT INTO st.study_titles(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.new_studies k
                    INNER JOIN " + schema_name + @".study_titles s
                    on k.sd_sid = s.sd_sid";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_titles", "new studies");
            _logger.Information("Transferred " + res.ToString() + " study titles, from 'preferred' studies");

            // For 'existing studies' study Ids add only new titles.

            if (nonpref_number > 0)
            {
                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_titles d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid";
                db.ExecuteSQL(sql_string);

                // Also, all non preferred titles must be non-default (default will be from the preferred source)

                sql_string = @"UPDATE nk.source_data 
                           SET is_default = false;";
                db.ExecuteSQL(sql_string);


                sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.title_text 
                           FROM st.study_titles c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id;";
                db.ExecuteSQL(sql_string);

                // for titles which are the same as some that already exist
                // the comments field should be updated to reflect this...

                sql_string = @"UPDATE st.study_titles t
                           set comments = t.comments || '; ' || s.comments 
                           FROM nk.source_data s
                           WHERE t.study_id = s.study_id
                           AND lower(t.title_text) = lower(s.title_text);";
                db.ExecuteSQL(sql_string);

                // for titles which are new to the study
                // simply add them

                sql_string = @"INSERT INTO st.study_titles(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND lower(s.title_text) = lower(e.title_text)
                           WHERE e.study_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "study_titles", "existing studies");
                _logger.Information("Transferred " + res.ToString() + " study titles, from 'non-preferred' studies");
            }
        }


        public void LoadStudyContributors(string schema_name)
        {
            _logger.Information("");

            string destination_field_list = @"study_id, 
            contrib_type_id, is_individual, 
            person_id, person_given_name, person_family_name, person_full_name,
            orcid_id, person_affiliation, organisation_id, 
            organisation_name, organisation_ror_id ";
            
            string source_field_list = @" 
            s.contrib_type_id, s.is_individual, 
            s.person_id, s.person_given_name, s.person_family_name, s.person_full_name,
            s.orcid_id, s.person_affiliation, s.organisation_id, 
            s.organisation_name, s.organisation_ror_id ";

            // For 'preferred' study Ids add all contributors.

            string sql_string = @"INSERT INTO st.study_contributors(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.new_studies k
                    INNER JOIN " + schema_name + @".study_contributors s
                    on k.sd_sid = s.sd_sid";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_contributors", "new studies");
            _logger.Information("Transferred " + res.ToString() + " study_contributors, from 'preferred' studies");

            // For 'existing studies' study Ids add only new contributors.
            // Need to do it in two sets to simplify the SQL (to try and avoid time outs)

            if (nonpref_number > 0)
            {

                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_contributors d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid
                           WHERE d.is_individual = false";
                db.ExecuteSQL(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.contrib_type_id, c.organisation_name 
                           FROM st.study_contributors c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id
                           WHERE c.is_individual = false";
                db.ExecuteSQL(sql_string);

                sql_string = @"INSERT INTO st.study_contributors(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.contrib_type_id = e.contrib_type_id
                           AND s.organisation_name = e.organisation_name
                           WHERE e.study_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "study_contributors", "existing studies");
                _logger.Information("Transferred " + res.ToString() + " study_contributors (orgs), from 'non-preferred' studies");

                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* FROM " + schema_name + @".study_contributors d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid
                           WHERE d.is_individual = true";
                db.ExecuteSQL(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.contrib_type_id, c.person_full_name 
                           FROM st.study_contributors c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id
                           WHERE c.is_individual = true";
                db.ExecuteSQL(sql_string);

                sql_string = @"INSERT INTO st.study_contributors(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.contrib_type_id = e.contrib_type_id
                           AND s.person_full_name = e.person_full_name
                           WHERE e.study_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "study_contributors", "existing studies");
                _logger.Information("Transferred " + res.ToString() + " study_contributors (people), from 'non-preferred' studies");
            }
        }


        public void LoadStudyTopics(string schema_name)
        {
            _logger.Information("");

            string destination_field_list = @"study_id, 
                    topic_type_id, mesh_coded, mesh_code, mesh_value, 
                    original_ct_id, original_ct_code, original_value ";

            string source_field_list = @" 
                    s.topic_type_id, s.mesh_coded, s.mesh_code, s.mesh_value, 
                    original_ct_id, original_ct_code, s.original_value ";

            // For 'preferred' study Ids add all topics.

            string sql_string = @"INSERT INTO st.study_topics(" + destination_field_list + @")
                SELECT k.study_id, " + source_field_list + @"
                FROM nk.new_studies k
                INNER JOIN " + schema_name + @".study_topics s
                    on k.sd_sid = s.sd_sid";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_topics", "new studies");
            _logger.Information("Transferred " + res.ToString() + " mesh coded study topics, from 'preferred' studies");

            // For 'existing studies' study Ids add only new topics.
            // Do this in two stages - for mesh coded data
            // and then for non-mesh coded data

            if (nonpref_number > 0)
            {
                // create existing data once...

                sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.mesh_value, c.original_value
                           FROM st.study_topics c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id;";
                db.ExecuteSQL(sql_string);

                // look at mesh coded new data

                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_topics d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid
                           WHERE mesh_coded = true";
                db.ExecuteSQL(sql_string);


                sql_string = @"INSERT INTO st.study_topics(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.mesh_value = e.mesh_value
                           WHERE e.study_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "study_topics", "existing studies");
                _logger.Information("Transferred " + res.ToString() + " mesh coded study_topics, from 'non-preferred' studies");

                // look at non mesh coded new data

                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_topics d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid
                           WHERE mesh_coded = false";
                db.ExecuteSQL(sql_string);


                sql_string = @"INSERT INTO st.study_topics(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND lower(s.original_value) = lower(e.original_value)
                           WHERE e.study_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "study_topics", "existing studies");
                _logger.Information("Transferred " + res.ToString() + " non mesh coded study_topics, from 'non-preferred' studies");
            }
        }


        public void LoadStudyFeatures(string schema_name)
        {
            _logger.Information("");

            // For 'preferred' study Ids add all features.
            string destination_field_list = @"study_id, 
                    feature_type_id, feature_value_id ";

            string source_field_list = @" 
                    s.feature_type_id, s.feature_value_id ";

            string sql_string = @"INSERT INTO st.study_features(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.new_studies k
                    INNER JOIN " + schema_name + @".study_features s
                    on k.sd_sid = s.sd_sid";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_features", "new studies");
            _logger.Information("Transferred " + res.ToString() + " study features, from 'preferred' studies");

            // For 'existing studies' study Ids add only new feature types.

            if (nonpref_number > 0)
            {

                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_features d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid";
                db.ExecuteSQL(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.feature_type_id
                           FROM st.study_features c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id;";
                db.ExecuteSQL(sql_string);

                sql_string = @"INSERT INTO st.study_features(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.feature_type_id = e.feature_type_id
                           WHERE e.study_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "study_features", "existing studies");
                _logger.Information("Transferred " + res.ToString() + " study_features, from 'non-preferred' studies");
            }
        }


        public void LoadStudyRelationShips(string schema_name)
        {
            _logger.Information("");

            string destination_field_list = @"study_id, 
                    relationship_type_id ";

            string source_field_list = @" 
                    s.relationship_type_id ";

            // For 'preferred' study Ids add all relationships.
            
            string sql_string = @"INSERT INTO st.study_relationships(" + destination_field_list + @")
                    SELECT k.study_id, " + source_field_list + @"
                    FROM nk.new_studies k
                    INNER JOIN " + schema_name + @".study_relationships s
                    on k.sd_sid = s.sd_sid";
            
            int res = db.ExecuteTransferSQL(sql_string, schema_name, "study_relationships", "new studies");
            _logger.Information("Transferred " + res.ToString() + " study relationships, from 'preferred' studies");

            // For 'existing studies' study Ids add only new relationships types.

            if (nonpref_number > 0)
            {
                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.study_id, d.* 
                           FROM " + schema_name + @".study_relationships d
                           INNER JOIN nk.existing_studies es
                           ON d.sd_sid = es.sd_sid";
                db.ExecuteSQL(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT es.sd_sid, es.study_id, 
                           c.relationship_type_id
                           FROM st.study_relationships c
                           INNER JOIN nk.existing_studies es
                           ON c.study_id = es.study_id;";
                db.ExecuteSQL(sql_string);

                sql_string = @"INSERT INTO st.study_relationships(" + destination_field_list + @")
                           SELECT s.study_id, " + source_field_list + @" 
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_sid = e.sd_sid
                           AND s.relationship_type_id = e.relationship_type_id
                           WHERE e.study_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "study_relationships", "existing studies");
                _logger.Information("Transferred " + res.ToString() + " study relationships, from 'non-preferred' studies");

                // insert target study id, using sd_sid to find it in the temp studies table
                // N.B. These relationships are defined within the same source...
                // (Cross source relationships are defined in the links (nk) schema)

                sql_string = @"UPDATE st.study_relationships r
                    SET target_study_id = tt.target_study_id
                    FROM 
                        (SELECT t.study_id, t2.study_id as target_study_id
                            FROM nk.temp_study_ids t 
                            INNER JOIN " + schema_name + @".study_relationships s 
                            on t.sd_sid = s.sd_sid
                            INNER JOIN nk.temp_study_ids t2
                            on s.target_sd_sid = t2.sd_sid) tt
                    WHERE r.study_id = tt.study_id ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "study_relationships", "updating target ids");
                _logger.Information("Updatedd records - " + res.ToString() + " study_relationships, updating target ids");
            }

        }

        public void DropTempStudyIdsTable()
        {
            string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_ids;
                                  DROP TABLE IF EXISTS nk.new_studies;
                                  DROP TABLE IF EXISTS nk.existing_studies;
                                  DROP TABLE IF EXISTS nk.source_data;
                                  DROP TABLE IF EXISTS nk.existing_data;";
            db.ExecuteSQL(sql_string);
        }

    }
}
