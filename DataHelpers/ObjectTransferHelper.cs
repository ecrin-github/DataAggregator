using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
using System.Collections.Generic;


namespace DataAggregator
{
    public class ObjectDataTransferrer
    {

        string _connString;
        DBUtilities db;
        LoggingHelper _loggingHelper;
        int num_to_check;

        int status1number, status2number, status3number;

        public ObjectDataTransferrer(string connString, LoggingHelper loggingHelper)
        {
            _connString = connString;
            _loggingHelper = loggingHelper;
            db = new DBUtilities(connString, _loggingHelper);
        }

        public void SetUpTempObjectIdsTables()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
                      CREATE TABLE IF NOT EXISTS nk.temp_object_ids(
                        id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                      , object_id                INT
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
                      );
                 CREATE INDEX temp_object_ids_objectid ON nk.temp_object_ids(object_id);
                 CREATE INDEX temp_object_ids_sdidsource ON nk.temp_object_ids(source_id, sd_oid);";

                conn.Execute(sql_string);
                 
                sql_string = @"DROP TABLE IF EXISTS nk.temp_objects_to_add;
                      CREATE TABLE IF NOT EXISTS nk.temp_objects_to_add(
                        id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                      , object_id                INT
                      , sd_oid                   VARCHAR
                      , object_type_id		  	 INT            
                      , title                    VARCHAR      
                      , is_preferred_object      BOOLEAN
                      ); 
                      CREATE INDEX temp_objects_to_add_sd_oid on nk.temp_objects_to_add(sd_oid);";

                conn.Execute(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.temp_objects_to_check;
                      CREATE TABLE IF NOT EXISTS nk.temp_objects_to_check(
                        id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                      , object_id                INT
                      , sd_oid                   VARCHAR
                      , object_type_id		  	 INT            
                      , title                    VARCHAR      
                      , is_preferred_object      BOOLEAN
                      ); 
                      CREATE INDEX temp_objects_to_check_sd_oid on nk.temp_objects_to_check(sd_oid);";

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


        public void MatchExistingObjectIds(int source_id)
        {
            // Do these source - object id combinations already exist in the system,
            // i.e. have a known id? If they do they can be matched, to leave only 
            // the new object ids to process

            string sql_string = @"UPDATE nk.temp_object_ids t
                    SET object_id = doi.object_id, 
                    is_preferred_object = doi.is_preferred_object,
                    parent_study_id = doi.parent_study_id,
                    is_preferred_study = doi.is_preferred_study,
                    match_status = 1
                    from nk.data_object_ids doi
                    where doi.source_id = " + source_id.ToString() + @"
                    and t.sd_oid = doi.sd_oid ";

            int res = db.Update_UsingTempTable("nk.temp_object_ids", "nk.temp_object_ids", sql_string, " and ");
            _loggingHelper.LogLine("Existing objects matched in temp table");

            // also update the data_object_identifiers table
            // Indicates has been matched and updates the 
            // data fetch date

            sql_string = @"UPDATE nk.data_object_ids doi
            set match_status = 1,
            datetime_of_data_fetch = t.datetime_of_data_fetch
            from nk.temp_object_ids t
            where doi.source_id = " + source_id.ToString() + @"
            and t.sd_oid = doi.sd_oid ";

            status1number = db.Update_UsingTempTable("nk.temp_object_ids", "data_object_identifiers", sql_string, " and ");
            _loggingHelper.LogLine(status1number.ToString() + " existing objects matched in identifiers table");
        }


        public void UpdateNewObjectsWithStudyIds(int source_id)
        {
            // For the new objects...where match_status still 0
            
            // Update the object parent study_id using the 'correct'
            // value found in the study_identifiers table

            string sql_string = @"UPDATE nk.temp_object_ids t
                        SET parent_study_id = si.study_id, 
                        is_preferred_study = si.is_preferred
                        FROM nk.study_ids si
                        WHERE t.parent_study_sd_sid = si.sd_sid
                        and t.parent_study_source_id = " + source_id.ToString();

            int res = db.ExecuteSQL(sql_string);
            _loggingHelper.LogLine("Objects pdated with parent study details");

            // Drop those object records that cannot be matched
            // N.B. study linked records - Pubmed objects do not 
            // travel down this path

            sql_string = @"DELETE FROM nk.temp_object_ids
                            WHERE parent_study_id is null;";
            res = db.ExecuteSQL(sql_string);
            _loggingHelper.LogLine(res.ToString() + " objects dropped because of a missing matching study");
        }


        public void AddNewObjectsToIdentifiersTable(int source_id)
        {
            // Add all the new object id records to the all Ids table

            string sql_string = @"INSERT INTO nk.data_object_ids
                            (source_id, sd_oid, object_type_id, title, is_preferred_object,
                            parent_study_source_id, parent_study_sd_sid,
                            parent_study_id, is_preferred_study, datetime_of_data_fetch)
                            select 
                            source_id, sd_oid, object_type_id, title, is_preferred_object,
                            parent_study_source_id, parent_study_sd_sid,
                            parent_study_id, is_preferred_study, datetime_of_data_fetch
                            from nk.temp_object_ids t
                            where match_status = 0 ";

            db.Update_UsingTempTable("nk.temp_object_ids", "data_object_identifiers", sql_string, " and ");
            _loggingHelper.LogLine("Non-matched objects inserted into object identifiers table");

            // For study based data, if the study is 'preferred' it is the first time
            // that it and related data objects can be added to the database, so
            // set the object id to the table id and set the match_status to 2

            sql_string = @"UPDATE nk.data_object_ids
                        SET object_id = id, is_preferred_object = true,
                        match_status = 3
                        WHERE object_id is null
                        AND source_id = " + source_id.ToString() + @"
                        AND is_preferred_study = true;";

            int res1 = db.ExecuteSQL(sql_string);
            _loggingHelper.LogLine(res1.ToString() + " new objects identified for addition from preferred studies");

            // For data objects from 'non-preferred' studies, there may be duplicate 
            // data objects already in the system, but that does not apply to registry
            // linked objects such as registry entries, results entries, web landing pages

            sql_string = @"UPDATE nk.data_object_ids
                        SET object_id = id, is_preferred_object = true,
                        match_status = 3
                        WHERE object_id is null
                        AND source_id = " + source_id.ToString() + @"
                        AND object_type_id in (13, 28);";
            int res2 = db.ExecuteSQL(sql_string);

            if (source_id == 101900 || source_id == 101901)  // BioLINCC or Yoda
            {
                sql_string = @"UPDATE nk.data_object_ids
                        SET object_id = id, is_preferred_object = true,
                        match_status = 3
                        WHERE object_id is null
                        AND source_id = " + source_id.ToString() + @"
                        AND object_type_id in (38);";
                res2 += db.ExecuteSQL(sql_string);
            }

            _loggingHelper.LogLine(res2.ToString() + " new 'always added' objects identified from non-preferred studies");
            status3number = res1 + res2;
        }


        public void CheckNewObjectsForDuplicateTitles(int source_id)
        {
            // Any more new object records to be checked?

            // duplicates may be picked up from 
            // considering type and title within the same study

            // First need to consider the (distinct) non-preferred studies
            // that currently have unmatched objects 

            string sql_string = @"drop table if exists nk.studies_with_poss_dup_objects;
                  create table nk.studies_with_poss_dup_objects
                  as 
                  select distinct parent_study_id 
                  from nk.data_object_ids
                  WHERE object_id is null
                  AND source_id = " + source_id.ToString();

            db.ExecuteSQL(sql_string);

            // get data object records that link to the same study 
            // excluding those in the table from the current source

            sql_string = @"drop table if exists nk.dup_objects_by_type_and_title;
                  create table nk.dup_objects_by_type_and_title
                  as 
                select existing.object_id as old_object_id, new.id 
                from
                (select doi.* from nk.studies_with_poss_dup_objects p
                   inner join nk.data_object_ids doi
                   on p.parent_study_id = doi.parent_study_id
                   where doi.parent_study_source_id <> " + source_id.ToString() + @") existing
                inner join
                (select doi2.* from nk.data_object_ids doi2
                  WHERE object_id is null
                  AND source_id = " + source_id.ToString() + @"
                ) new
                on existing.parent_study_id = new.parent_study_id
                and existing.object_type_id = new.object_type_id
                and existing.title = new.title";

            db.ExecuteSQL(sql_string);

            sql_string = @"Update nk.data_object_ids doi
            set object_id = old_object_id, is_preferred_object = false, 
            is_valid_link = false, match_status = 2
            from nk.dup_objects_by_type_and_title dup
            where doi.match_status is null
            and doi.id = dup.id;";

            status2number = db.ExecuteSQL(sql_string);
            _loggingHelper.LogLine(status2number.ToString() + " objects from 'non-preferred' studies identified as duplicates using type and title");

        }


        public void CheckNewObjectsForDuplicateURLs(int source_id, string schema_name)
        {
            // Any more new object records to be checked

            // duplicates may be picked up  
            // by considering the same URL within the same study

            // Use the (distinct) studies that currently have
            // unmatched objects (table still in existence)

            string sql_string = @"drop table if exists nk.dup_objects_by_url;
                  create table nk.dup_objects_by_url
                  as 
                  select 
                     existing.object_id as old_object_id, new.id 
                     from
                 (select doi.*, i.url from nk.studies_with_poss_dup_objects p
                   inner join nk.data_object_ids doi
                   on p.parent_study_id = doi.parent_study_id
                   inner join ob.object_instances i
                   on doi.object_id = i.object_id
                   where i.url is not null
                  ) existing
                 inner join
                 (select doi2.*, i.url from nk.data_object_ids doi2
                  inner join " + schema_name + @".object_instances i
                  on doi2.sd_oid = i.sd_oid
                  WHERE doi2.object_id is null
                  AND doi2.source_id = " + source_id.ToString() + @"
                 ) new
                 on existing.parent_study_id = new.parent_study_id
                 and existing.url = new.url";

            db.ExecuteSQL(sql_string);

            sql_string = @"Update nk.data_object_ids doi
            set object_id = old_object_id, is_preferred_object = false, 
            is_valid_link = false, match_status = 2
            from nk.dup_objects_by_url dup
            where doi.match_status is null
            and doi.id = dup.id;";

            int res = db.ExecuteSQL(sql_string);
            _loggingHelper.LogLine(res.ToString() + " objects from 'non-preferred' studies identified as duplicates using url");
            status2number += res;

             // tidy up temp tables
             sql_string = @"drop table if exists nk.studies_with_poss_dup_objects;
                         drop table if exists nk.dup_objects_by_type_and_title;
                         drop table if exists nk.dup_objects_by_url";
            db.ExecuteSQL(sql_string);
        }


        public void CompleteNewObjectsStatuses(int source_id)
        {
            // complete setting status of new objects
            // Any still null must be genuinely new

            string sql_string = @"Update nk.data_object_ids doi
            set object_id = id, is_preferred_object = true, 
            match_status = 3
            where doi.match_status is null
            AND source_id = " + source_id.ToString();

            int res = db.ExecuteSQL(sql_string);
            _loggingHelper.LogLine(res.ToString() + " remaining objects from 'non-preferred' studies identified as new objects");
            status3number += res;
        }


        public void FillObjectsToAddTables(int source_id)
        {
            int total_objects = status1number + status2number + status3number;
            _loggingHelper.LogLine(total_objects.ToString() + " total objects found");
            
            string sql_string = @"INSERT INTO nk.temp_objects_to_add
                            (object_id, sd_oid)
                            SELECT distinct object_id, sd_oid 
                            FROM nk.data_object_ids
                            WHERE is_preferred_object = true and 
                            source_id = " + source_id.ToString();
            
            int res = db.ExecuteSQL(sql_string);
            _loggingHelper.LogLine(res.ToString() + " objects to be added");

            sql_string = @"INSERT INTO nk.temp_objects_to_check
                            (object_id, sd_oid)
                            SELECT distinct object_id, sd_oid 
                            FROM nk.data_object_ids
                            WHERE is_preferred_object = false
                            and source_id = " + source_id.ToString();

            res = db.ExecuteSQL(sql_string);
            num_to_check = res;
            if (num_to_check > 0)
            {
                _loggingHelper.LogLine(res.ToString() + " objects identifies as likely duplicates");
                _loggingHelper.LogLine("will not be added (but attributes may be checked)");
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

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "data_objects", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " data object records");
            return res;
        }

    
        public void LoadObjectDatasets(string schema_name)
        {
            _loggingHelper.LogLine("");

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

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_datasets", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " object datasets");
        }


        public void LoadObjectInstances(string schema_name)
        {
            _loggingHelper.LogLine("");

            string destination_field_list = @"object_id,  
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, resource_comments ";

            string source_field_list = @" 
            s.instance_type_id, s.repository_org_id, s.repository_org,
            s.url, s.url_accessible, s.url_last_checked, s.resource_type_id,
            s.resource_size, s.resource_size_units, s.resource_comments "; 
            
            // add the instances known to require adding

            string sql_string = @"INSERT INTO ob.object_instances(" + destination_field_list + @") 
            SELECT t.object_id, " + source_field_list + @"
            FROM " + schema_name + @".object_instances s
            INNER JOIN nk.temp_objects_to_add t
            on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_instances", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " object instances, from 'preferred' objects");

            if (num_to_check > 0)
            {
                // add any additional instances (i.e. that have a URL not already present)
                // from objects linked to non-preferred versions of studies...

                // source data is the instance data associated with these
                // particular objects

                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                           CREATE TABLE nk.source_data as 
                           SELECT es.object_id, d.* 
                           FROM " + schema_name + @".object_instances d
                           INNER JOIN nk.temp_objects_to_check es
                           ON d.sd_oid = es.sd_oid";
                db.ExecuteSQL(sql_string);

                // destination data (to check against) is the current set of URLs
                // in the existing instance data for these objects

                sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                           CREATE TABLE nk.existing_data as 
                           SELECT k.sd_oid, 
                           c.object_id, c.url
                           FROM ob.object_instances c
                           INNER JOIN nk.temp_objects_to_check k
                           ON c.object_id = k.object_id;";
                db.ExecuteSQL(sql_string);

                // for urls which are new to the study - i.e. do not appear io the RHS
                // of a LEFT JOIN on oid and url, add the instance data as a new record

                sql_string = @"INSERT INTO ob.object_instances(" + destination_field_list + @")
                           SELECT s.object_id, " + source_field_list + @"
                           FROM nk.source_data s
                           LEFT JOIN nk.existing_data e
                           ON s.sd_oid = e.sd_oid
                           and lower(s.url) = lower(e.url)
                           WHERE e.object_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "object_instances", "existing objects");
                _loggingHelper.LogLine("Transferred " + res.ToString() + " object instances, from 'non-preferred'  objects");
            }
        }


        public void LoadObjectTitles(string schema_name)
        {
            _loggingHelper.LogLine("");

            string destination_field_list = @"object_id, 
            title_type_id, title_text, lang_code,
            lang_usage_id, is_default, comments ";

            string source_field_list = @" 
            s.title_type_id, s.title_text, s.lang_code,
            s.lang_usage_id, s.is_default, s.comments "; 
            
            string sql_string = @"INSERT INTO ob.object_titles(" + destination_field_list + @") 
            SELECT t.object_id, " + source_field_list + @"
            FROM " + schema_name + @".object_titles s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_titles", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " object titles, from 'preferred' objects");

            if (num_to_check > 0)
            {
                // add any additional titles 

                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                               CREATE TABLE nk.source_data as 
                               SELECT es.object_id, d.* 
                               FROM " + schema_name + @".object_titles d
                               INNER JOIN nk.temp_objects_to_check es
                               ON d.sd_oid = es.sd_oid";
                db.ExecuteSQL(sql_string);

                // Also, all non preferred titles must be non-default (default will be from the preferred source)

                sql_string = @"UPDATE nk.source_data 
                               SET is_default = false;";
                db.ExecuteSQL(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                               CREATE TABLE nk.existing_data as 
                               SELECT k.sd_oid, 
                               c.object_id, c.title_text 
                               FROM ob.object_titles c
                               INNER JOIN nk.temp_objects_to_check k
                               ON c.object_id = k.object_id;";
                db.ExecuteSQL(sql_string);

                // for titles which are the same as some that already exist
                // the comments field should be updated to reflect this...

                sql_string = @"UPDATE ob.object_titles t
                               set comments = t.comments || '; ' || s.comments 
                               FROM nk.source_data s
                               WHERE t.object_id = s.object_id
                               AND lower(t.title_text) = lower(s.title_text);";
                db.ExecuteSQL(sql_string);

                // for titles which are new to the study (have null on the RHS of a
                // LEFT JOIN on oid and title text, simply add them

                sql_string = @"INSERT INTO ob.object_titles(" + destination_field_list + @")
                               SELECT s.object_id, " + source_field_list + @" 
                               FROM nk.source_data s
                               LEFT JOIN nk.existing_data e
                               ON s.sd_oid = e.sd_oid
                               AND lower(s.title_text) = lower(e.title_text)
                               WHERE e.object_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "object_titles", "existing objects");
                _loggingHelper.LogLine("Transferred " + res.ToString() + " object titles, from 'non-preferred' objects");

            }
        }


        public void LoadObjectDates(string schema_name)
        {
            _loggingHelper.LogLine("");

            string destination_field_list = @"object_id, 
            date_type_id, date_is_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details ";

            string source_field_list = @" 
            s.date_type_id, s.date_is_range, s.date_as_string, s.start_year, 
            s.start_month, s.start_day, s.end_year, s.end_month, s.end_day, s.details ";

            string sql_string = @"INSERT INTO ob.object_dates(" + destination_field_list + @")
            SELECT t.object_id, " + source_field_list + @"
                       FROM " + schema_name + @".object_dates s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_dates", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " object dates, from 'preferred' objects");

            if (num_to_check > 0)
            {
                // add any additional dates 

                sql_string = @"DROP TABLE IF EXISTS nk.source_data;
                               CREATE TABLE nk.source_data as 
                               SELECT es.object_id, d.* 
                               FROM " + schema_name + @".object_dates d
                               INNER JOIN nk.temp_objects_to_check es
                               ON d.sd_oid = es.sd_oid";
                db.ExecuteSQL(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.existing_data;
                               CREATE TABLE nk.existing_data as 
                               SELECT k.sd_oid, 
                               c.object_id, c.start_year, c.start_month, c.start_day
                               FROM ob.object_dates c
                               INNER JOIN nk.temp_objects_to_check k
                               ON c.object_id = k.object_id;";
                db.ExecuteSQL(sql_string);

                // for dates which are new to the study (have null on the RHS of a
                // LEFT JOIN on oid and year, month and day, add them

                sql_string = @"INSERT INTO ob.object_dates(" + destination_field_list + @")
                               SELECT s.object_id, " + source_field_list + @" 
                               FROM nk.source_data s
                               LEFT JOIN nk.existing_data e
                               ON s.sd_oid = e.sd_oid
                               AND s.start_year = e.start_year
                               AND s.start_month = e.start_month
                               AND s.start_day = e.start_day
                               WHERE e.object_id is null ";

                res = db.ExecuteTransferSQL(sql_string, schema_name, "object_dates", "existing objects");
                _loggingHelper.LogLine("Transferred " + res.ToString() + " object dates, from 'non-preferred' objects");



            }
        }


        public void LoadObjectContributors(string schema_name)
        {
            _loggingHelper.LogLine("");

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

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_contributors", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " object contributors, from 'preferred' objects");
        }


        public void LoadObjectTopics(string schema_name)
        {
            _loggingHelper.LogLine("");

            string sql_string = @"INSERT INTO ob.object_topics(object_id, 
            topic_type_id, mesh_coded, mesh_code, mesh_value, 
            original_ct_id, original_ct_code, original_value)
            SELECT t.object_id, 
            topic_type_id, mesh_coded, mesh_code, mesh_value, 
            original_ct_id, original_ct_code, original_value
            FROM " + schema_name + @".object_topics s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_topics", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " object topics, from 'preferred' objects");
        }


        public void LoadObjectDescriptions(string schema_name)
        {
            _loggingHelper.LogLine("");

            string sql_string = @"INSERT INTO ob.object_descriptions(object_id, 
            description_type_id, label, description_text, lang_code)
            SELECT t.object_id,
            description_type_id, label, description_text, lang_code
            FROM " + schema_name + @".object_descriptions s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_descriptions", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " object descriptions, from 'preferred' objects");
        }


        public void LoadObjectIdentifiers(string schema_name)
        {
            _loggingHelper.LogLine("");

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

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_identifiers", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " object identifiers, from 'preferred' objects");
        }



        public void LoadObjectRelationships(string schema_name)
        {
            _loggingHelper.LogLine("");

            string sql_string = @"INSERT INTO ob.object_relationships(object_id,  
            relationship_type_id)
            SELECT t.object_id,
            relationship_type_id
            FROM " + schema_name + @".object_relationships s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            // NEED TO DO UPDATE OF TARGET SEPARATELY

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_relationships", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " object relationships, from 'preferred' objects");
        }



        public void LoadObjectRights(string schema_name)
        {
            _loggingHelper.LogLine("");

            string sql_string = @"INSERT INTO ob.object_rights(object_id,  
            rights_name, rights_uri, comments)
            SELECT t.object_id,
            rights_name, rights_uri, comments
            FROM " + schema_name + @".object_rights s
                    INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

            int res = db.ExecuteTransferSQL(sql_string, schema_name, "object_rights", "new objects");
            _loggingHelper.LogLine("Transferred " + res.ToString() + " object rights, from 'preferred' objects");
        }


        public void DropTempObjectIdsTable()
        {
            string sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
                                  DROP TABLE IF EXISTS nk.temp_objects_to_add;
                                  DROP TABLE IF EXISTS nk.source_data;
                                  DROP TABLE IF EXISTS nk.existing_data;
                                  DROP TABLE IF EXISTS nk.temp_objects_to_check;";
            db.ExecuteSQL(sql_string);
        }

    }
}
