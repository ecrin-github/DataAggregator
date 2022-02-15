using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using Serilog;

namespace DataAggregator
{
    class PubmedTransferHelper
    {
        private string _connString;
        private string _schema_name;
        DBUtilities db;
        ILogger _logger;

        public PubmedTransferHelper(string schema_name, string connString, ILogger logger)
        {
            _schema_name = schema_name;
            _connString = connString;
            _logger = logger;
            db = new DBUtilities(connString, _logger);
        }

        // Tables and functions used for the PMIDs collected from DB Sources

        public void SetupTempPMIDTables()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"DROP TABLE IF EXISTS nk.temp_pmids;
                      CREATE TABLE IF NOT EXISTS nk.temp_pmids(
                        id                       INT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                      , source_id                INT
                      , sd_oid                   VARCHAR
                      , parent_study_source_id   INT 
                      , parent_study_sd_sid      VARCHAR
                      , datetime_of_data_fetch   TIMESTAMPTZ
                      ); ";
                conn.Execute(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.distinct_temp_object_ids;
                      CREATE TABLE IF NOT EXISTS nk.distinct_temp_object_ids(
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
                 CREATE INDEX disttemp_object_ids_objectid ON nk.temp_object_ids(object_id);
                 CREATE INDEX disttemp_object_ids_sdidsource ON nk.temp_object_ids(source_id, sd_oid);";

                conn.Execute(sql_string);
            }
        }

        public IEnumerable<PMIDLink> FetchBankPMIDs()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"select 
                        100135 as source_id, 
                        d.id as parent_study_source_id, 
                        k.sd_oid, k.id_in_db as parent_study_sd_sid, 
                        a.datetime_of_data_fetch
                        from " + _schema_name + @".object_db_links k
                        inner join " + _schema_name + @".data_objects a 
                        on k.sd_oid = a.sd_oid
                        inner join context_ctx.nlm_databanks d
                        on k.db_name = d.nlm_abbrev
                        where d.id not in (100156, 100157, 100158)";
                return conn.Query<PMIDLink>(sql_string);
            }
        }

        public ulong StorePMIDLinks(PostgreSQLCopyHelper<PMIDLink> copyHelper, 
                                    IEnumerable<PMIDLink> entities)
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }

        public void CleanPMIDsdsidData()
        {
            CleanPMIDsdsidData1();
            CleanPMIDsdsidData2();
            CleanPMIDsdsidData3();
            CleanPMIDsdsidData4();
        }

        private void CleanPMIDsdsidData1() 
        {
            string sql_string = "";
            using (var conn = new NpgsqlConnection(_connString))
            {
                sql_string = @"UPDATE nk.temp_pmids
                        SET parent_study_sd_sid = 'ACTRN' || parent_study_sd_sid
                        WHERE parent_study_source_id = 100116
                        AND length(parent_study_sd_sid) = 14;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, ' ', '')
                        WHERE parent_study_source_id = 100116;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, '#', '')
                        WHERE parent_study_source_id = 100116;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, ':', '')
                        WHERE parent_study_source_id = 100116;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, '[', '')
                        WHERE parent_study_source_id = 100116;";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.temp_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'CHICTR', 'ChiCTR')
                        WHERE parent_study_source_id = 100118;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                        SET parent_study_sd_sid = 'ChiCTR-' || parent_study_sd_sid
                        WHERE parent_study_source_id = 100118
                        and parent_study_sd_sid not ilike 'ChiCTR-%';";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'ChiCTR-ChiCTR', 'ChiCTR-')
                        WHERE parent_study_source_id = 100118;";
                conn.Execute(sql_string);
            }
        }

        private void CleanPMIDsdsidData2()
        {
            string sql_string = "";
            using (var conn = new NpgsqlConnection(_connString))
            {
                sql_string = @"UPDATE nk.temp_pmids
                     SET parent_study_sd_sid = Replace(parent_study_sd_sid, '/', '-')
                     WHERE parent_study_source_id = 100121;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                     SET parent_study_sd_sid = 'CTRI-' || parent_study_sd_sid
                     WHERE parent_study_source_id = 100121
                     and parent_study_sd_sid not ilike 'CTRI-%';";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                     SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'REF-', '')
                     WHERE parent_study_source_id = 100121;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                     SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'CTRI-CTRI', 'CTRI-')
                     WHERE parent_study_source_id = 100121;";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = 'RPCEC' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100122
                   and parent_study_sd_sid not ilike 'RPCEC%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = UPPER(parent_study_sd_sid)
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '–', '-')
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EUDRA-CT', 'EUDRACT')
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EUDRACT', '')
                    WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EURODRACT', '')
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EU', '')
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'CT', '')
                   WHERE parent_study_source_id = 100123;";

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = left(parent_study_sd_sid, 14)
                   WHERE parent_study_source_id = 100123
                   and length(parent_study_sd_sid) > 14;";
                conn.Execute(sql_string);
            }
        }

        private void CleanPMIDsdsidData3()
        {
            string sql_string = "";
            using (var conn = new NpgsqlConnection(_connString))
            {
                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = Replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100124;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = Replace(parent_study_sd_sid, '-', '')
                   WHERE parent_study_source_id = 100124;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'DKRS', 'DRKS')
                   WHERE parent_study_source_id = 100124;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'DRK0', 'DRKS0')
                   WHERE parent_study_source_id = 100124;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = 'DRKS' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100124
                   and parent_study_sd_sid not ilike 'DRKS%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = 'IRCT' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100125
                   and parent_study_sd_sid not ilike 'IRCT%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'SRCTN', 'ISRCTN')
                   WHERE parent_study_source_id = 100126
                   and parent_study_sd_sid ilike 'SRCTN%';";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'ISRTN', 'ISRCTN')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'ISRNT', 'ISRCTN')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = 'ISRCTN' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100126
                   and parent_study_sd_sid not ilike 'ISRCTN%';";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '#', '')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '/', '')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ':', '')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

            }
        }

        private void CleanPMIDsdsidData4()
        {
            string sql_string = "";
            using (var conn = new NpgsqlConnection(_connString))
            {
                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100128;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'PACTCR', 'PACTR')
                   WHERE parent_study_source_id = 100128;";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = 'PACTR' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100128
                   and parent_study_sd_sid not ilike 'PACTR%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '/', '-')
                   WHERE parent_study_source_id = 100130;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = 'SLCTR-' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100130
                   and parent_study_sd_sid not ilike 'SLCTR-%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '-', '')
                   WHERE parent_study_source_id = 100131;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100131;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = 'TCTR' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100131
                   and parent_study_sd_sid not ilike 'TCTR%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'NTRR', 'NTR')
                   WHERE parent_study_source_id = 100132;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100132;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '-', '')
                   WHERE parent_study_source_id = 100132;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.temp_pmids
                   SET parent_study_sd_sid = 'NTR' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100132
                   and parent_study_sd_sid not ilike 'NTR%'
                   and parent_study_sd_sid not ilike 'NL%';";
                conn.Execute(sql_string);

            }
        }


        public void TransferReferencesData(int source_id)
        {
            // This called only during testing
            // Transfers the study and study refernce records so that they can be used to
            // obtain pmids, in imitation of the normal process

            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"truncate table ad.studies; 
                    INSERT INTO ad.studies (sd_sid, display_title,
                    title_lang_code, brief_description, data_sharing_statement,
                    study_start_year, study_start_month, study_type_id,
                    study_status_id, study_enrolment, study_gender_elig_id, 
                    min_age, min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
                    record_hash, study_full_hash) 
                    SELECT sd_sid, display_title,
                    title_lang_code, brief_description, data_sharing_statement,
                    study_start_year, study_start_month, study_type_id, 
                    study_status_id, study_enrolment, study_gender_elig_id, 
                    min_age, min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
                    record_hash, study_full_hash 
                    FROM adcomp.studies
                    where source_id = " + source_id.ToString();
                conn.Execute(sql_string);


                sql_string = @"truncate table ad.study_references; 
                    INSERT INTO ad.study_references(sd_sid,
                       pmid, citation, doi, comments, record_hash)
                    SELECT sd_sid,
                       pmid, citation, doi, comments, record_hash
                       FROM adcomp.study_references
                       where source_id = " + source_id.ToString();
                conn.Execute(sql_string);
            }
        }


        public IEnumerable<PMIDLink> FetchSourceReferences(int source_id, string source_conn_string)
        {
            using (var conn = new NpgsqlConnection(source_conn_string))
            {
                string sql_string = @"select 
                        100135 as source_id, " +
                        source_id.ToString() + @" as parent_study_source_id, 
                        r.pmid as sd_oid, r.sd_sid as parent_study_sd_sid, 
                        s.datetime_of_data_fetch
                        from ad.study_references r
                        inner join ad.studies s
                        on r.sd_sid = s.sd_sid
                        where r.pmid is not null;";
                return conn.Query<PMIDLink>(sql_string);
            }
        }


        public void TransferPMIDLinksToTempObjectIds()
        {
                string sql_string = @"INSERT INTO nk.temp_object_ids(
                         source_id, sd_oid, object_type_id, 
                         parent_study_source_id, 
                         parent_study_sd_sid, datetime_of_data_fetch)
                         SELECT  
                         source_id, trim(sd_oid), 11, 
                         parent_study_source_id, 
                         parent_study_sd_sid, datetime_of_data_fetch
                         FROM nk.temp_pmids t ";
            int res = db.Update_UsingTempTable("nk.temp_pmids", "nk.temp_object_ids", sql_string, " where ");
            _logger.Information(res.ToString() + " PMID-study references passed to temp object table");
        }


        public void UpdateTempObjectIdsWithStudyDetails()
        {
            // first put in study id and whether is preferred, from 
            // the study_identifiers table

            string sql_string = @"UPDATE nk.temp_object_ids t
                    SET 
                    parent_study_id = si.study_id,
                    is_preferred_study = si.is_preferred
                    from nk.study_identifiers si
                    where t.parent_study_source_id = si.source_id
                    and t.parent_study_sd_sid = si.sd_sid ";

            int res = db.Update_UsingTempTable("nk.temp_object_ids", "nk.temp_object_ids", sql_string, " and ");
            _logger.Information(res.ToString() + " existing objects matched in temp table");

            // Some pubmed entries are not matched as the ids in the pubmed bank
            // data are still non-standard, or refer to obsolete ids (about 2500 in total)

            sql_string = @"DELETE from nk.temp_object_ids t
                       where t.parent_study_id is null;";
            res = db.ExecuteSQL(sql_string);
            _logger.Information(res.ToString() + " non matched PMID-studies deleted from total");

            // Then convert sd_sid and source to the preferred version - all equivalent
            // studies therefore have the same study data.
            // Increases the duplication if a paper has been cited in two or more study sources
            // but makes it easier to eliminate it using a later 'select distinct' call

            sql_string = @"UPDATE nk.temp_object_ids t
                    SET 
                    parent_study_source_id = si.source_id,
                    parent_study_sd_sid = si.sd_sid
                    from 
                    nk.study_identifiers si
                    where t.parent_study_id = si.study_id
                    and si.is_preferred = true ";

            res = db.Update_UsingTempTable("nk.temp_object_ids", "nk.temp_object_ids", sql_string, " and ");
            _logger.Information(res.ToString() + " existing objects matched in temp table");
        }


        public void FillDistinctTempObjectsTable()
        {
            // Then transfer the distinct data - to get the set of all 
            // the study - PMID combinations

            string sql_string = @"INSERT INTO nk.distinct_temp_object_ids(
                        source_id, sd_oid, object_type_id, parent_study_source_id, 
                        parent_study_sd_sid, parent_study_id, is_preferred_study)
                        SELECT DISTINCT
                        source_id, sd_oid, object_type_id, parent_study_source_id, 
                        parent_study_sd_sid, parent_study_id, is_preferred_study
                        FROM nk.temp_object_ids ";
            int res = db.ExecuteSQL(sql_string);
            _logger.Information(res.ToString() + " distinct study - PMID links collected together in single table");

            // object_id, title, is_preferred_object, datetime_of_data_fetch, match_status
            // all null at present
            // Update with latest datetime_of_data_fetch for each study- PMID combination

            sql_string = @"UPDATE nk.distinct_temp_object_ids dp
                        set datetime_of_data_fetch = mx.max_fetch_date
                        FROM 
                            (select parent_study_id, sd_oid,
                             max(datetime_of_data_fetch) as max_fetch_date
                             FROM nk.temp_object_ids
                             group by parent_study_id, sd_oid) mx
                        WHERE dp.parent_study_id = mx.parent_study_id
                        and dp.sd_oid = mx.sd_oid ";
            res = db.ExecuteSQL(sql_string);
            _logger.Information(res.ToString() + " record updated with latest date of data fetch");

        }

        public void MatchExistingPMIDLinks()
        {
            // identify the records in the temp table
            string sql_string = @"UPDATE nk.distinct_temp_object_ids t
            set object_id = doi.object_id, 
            is_preferred_object = doi.is_preferred_object,
            match_status = 1
            from nk.data_object_identifiers doi
            where t.parent_study_id = doi.parent_study_id
            and t.sd_oid = doi.sd_oid ";

            int res = db.Update_UsingTempTable("nk.distinct_temp_object_ids", "nk.distinct_temp_object_ids", sql_string, " and ");
            _logger.Information(res.ToString() + " existing objects matched in temp table");

            // update the matched records in the data object identifier table

            sql_string = @"UPDATE nk.data_object_identifiers doi
            set match_status = 1,
            datetime_of_data_fetch = t.datetime_of_data_fetch
            from nk.distinct_temp_object_ids t
            where doi.parent_study_id = t.parent_study_id
            and doi.sd_oid = t.sd_oid ";

            res = db.Update_UsingTempTable("nk.distinct_temp_object_ids", "data_object_identifiers", sql_string, " and ");
            _logger.Information(res.ToString() + " existing objects matched in identifiers table");
        }


        public void IdentifyNewPMIDLinkTypes()
        {
            // First insert title from the pubmed DB data objects

            string sql_string = @"Drop table if exists nk.pub_titles;
            Create table nk.pub_titles as select sd_oid, title
                     from " + _schema_name + @".data_objects ";
            db.ExecuteSQL(sql_string);

            sql_string = @"UPDATE nk.distinct_temp_object_ids t
            SET title = pt.title
            FROM nk.pub_titles pt
            where t.sd_oid = pt.sd_oid
            and t.match_status = 0 ";

            int res = db.Update_UsingTempTable("nk.distinct_temp_object_ids", "nk.distinct_temp_object_ids", sql_string, " and ");
            _logger.Information(res.ToString() + " unmatched PMID-study combinations updated with object titles");

            // identify and label completely new PMIDs first

            sql_string = @"Drop table if exists nk.missing_pmids;
            Create table nk.missing_pmids as 
                     select p.id
                     from nk.distinct_temp_object_ids p
                     left join nk.data_object_identifiers doi
                     on p.sd_oid = doi.sd_oid
                     where doi.sd_oid is null; ";

            db.ExecuteSQL(sql_string);

            sql_string = @"UPDATE nk.distinct_temp_object_ids t
            set match_status = 3
            FROM nk.missing_pmids m
            where t.id = m.id
            and t.match_status = 0 ";

            res = db.Update_UsingTempTable("nk.distinct_temp_object_ids", "nk.distinct_temp_object_ids", sql_string, " and ");
            _logger.Information(res.ToString() + " new PMID-study combinations found with completely new PMIDs");

            // Then identify records where the PMID exists but the link with that study is not
            // yet in the data_object_identifiers table

            sql_string = @"Drop table if exists nk.missing_links;
            Create table nk.missing_links as 
            select p.id
            from nk.distinct_temp_object_ids p
            left join nk.data_object_identifiers doi
            on p.sd_oid = doi.sd_oid
            and p.parent_study_id = doi.parent_study_id
            where doi.sd_oid is null";

            db.ExecuteSQL(sql_string);

            sql_string = @"UPDATE nk.distinct_temp_object_ids t
            set match_status = 2
            from nk.missing_links k 
            where t.match_status = 0
            and t.id = k.id ";

            res = db.Update_UsingTempTable("nk.distinct_temp_object_ids", "nk.distinct_temp_object_ids", sql_string, " and ");
            _logger.Information(res.ToString() + " new PMID-study combinations found for existing PMIDs");

        }

        public void AddNewPMIDStudyLinks()
        {
            // New links found for PMIDs already in the system
            // Get the preferred object id for this PMID link

             string sql_string = @"UPDATE nk.distinct_temp_object_ids t
             SET object_id = doi.object_id,
             is_preferred_object = false
             FROM nk.data_object_identifiers doi
             where t.sd_oid = doi.sd_oid
             and t.parent_study_id = doi.parent_study_id
             and doi.is_preferred_object = true
             and t.match_status = 2 ";

             int res = db.Update_UsingTempTable("nk.distinct_temp_object_ids", "nk.distinct_temp_object_ids", sql_string, " and ");
             _logger.Information(res.ToString() + " new PMID-study combinations updated");

             sql_string = @"Insert into nk.data_object_identifiers
             (object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                            parent_study_source_id, parent_study_sd_sid,
                            parent_study_id, is_preferred_study, datetime_of_data_fetch, match_status)
             select object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                            parent_study_source_id, parent_study_sd_sid,
                            parent_study_id, is_preferred_study, datetime_of_data_fetch, match_status
             FROM nk.distinct_temp_object_ids t
             where t.match_status = 2 ";

             res = db.Update_UsingTempTable("nk.distinct_temp_object_ids", "nk.data_object_identifiers", sql_string, " and ");
            _logger.Information(res.ToString() + " new PMID-study combinations added");

        }

        public void AddCompletelyNewPMIDs()
        {
            // Use the PMID-Study combination with the minimum study_id
            // as the 'preferred' object record for this PMID

            string sql_string = @"UPDATE nk.distinct_temp_object_ids t
             SET 
             is_preferred_object = true
             FROM 
                (select sd_oid, min(parent_study_id) as ms
                from nk.distinct_temp_object_ids 
                where match_status = 3
                group by sd_oid) m
             where t.sd_oid = m.sd_oid
             and t.parent_study_id = m.ms
             and t.match_status = 3 ";

            int res = db.ExecuteSQL(sql_string);
            _logger.Information(res.ToString() + " studies set as 'preferred' for new PMIDs");

            // Put the remaining study-PMID combinations as non-preferred

            sql_string = @"UPDATE nk.distinct_temp_object_ids t
            SET is_preferred_object = false
            where is_preferred_object is null
            and t.match_status = 3 ";

            res = db.ExecuteSQL(sql_string);
            _logger.Information(res.ToString() + " studies set as 'non-preferred' for new PMIDs");

            // add in the preferred new PMID records

            sql_string = @"Insert into nk.data_object_identifiers
            (source_id, sd_oid, object_type_id, title, is_preferred_object,
                     parent_study_source_id, parent_study_sd_sid,
                     parent_study_id, is_preferred_study, 
                     datetime_of_data_fetch, match_status)
             select source_id, sd_oid, object_type_id, title, is_preferred_object,
                     parent_study_source_id, parent_study_sd_sid,
                     parent_study_id, is_preferred_study, 
                     datetime_of_data_fetch, match_status
             FROM nk.distinct_temp_object_ids t
             where t.match_status = 3 
             and is_preferred_object = true";

            res = db.ExecuteSQL(sql_string);
            _logger.Information(res.ToString() + " new 'preferred' PMID-study combinations added for new PMIDs");

            // Update newly added records with object ids

            sql_string = @"Update nk.data_object_identifiers doi
            set object_id = id
            where match_status = 3 
            and source_id = 100135
            and object_id is null
            and is_preferred_object = true;";

            res = db.ExecuteSQL(sql_string);
            _logger.Information(res.ToString() + " object ids created for new PMIDs and applied to 'preferred' objects");

            // Update remaining study-PMID combinations with new object id

            sql_string = @"UPDATE nk.distinct_temp_object_ids t
             SET 
               object_id = doi.object_id
             FROM nk.data_object_identifiers doi
             where t.sd_oid = doi.sd_oid
             and t.parent_study_id = doi.parent_study_id
             and t.match_status = 3 
             and t.object_id is null ";

            res = db.Update_UsingTempTable("nk.distinct_temp_object_ids", "nk.data_object_identifiers", sql_string, " and ");
            _logger.Information(res.ToString() + " object ids applied to new PMIDs and 'non-preferred' objects");

            // Add remaining matching study-PMIDs records

            sql_string = @"Insert into nk.data_object_identifiers
            (object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                     parent_study_source_id, parent_study_sd_sid,
                     parent_study_id, is_preferred_study, 
                     datetime_of_data_fetch, match_status)
             select object_id, source_id, sd_oid, object_type_id, title, is_preferred_object,
                     parent_study_source_id, parent_study_sd_sid,
                     parent_study_id, is_preferred_study, 
                     datetime_of_data_fetch, match_status
             FROM nk.distinct_temp_object_ids t
             where t.match_status = 3 
             and is_preferred_object = false ";

            res = db.ExecuteSQL(sql_string);
            _logger.Information(res.ToString() + " new 'non-preferred' PMID-study combinations added");
        }


        public void IdentifyPMIDDataForImport(int source_id)
        {
            string sql_string = @"Insert into nk.temp_objects_to_add
                 (object_id, sd_oid)
                 select distinct object_id, sd_oid
                 from nk.data_object_identifiers doi
                 WHERE is_preferred_object = true and 
                 source_id = " + source_id.ToString();

            int res = db.ExecuteSQL(sql_string);
        }


        public void DropTempPMIDTables()
        {
            using (var conn = new NpgsqlConnection(_connString))
            {
                string sql_string = @"DROP TABLE IF EXISTS nk.temp_pmids;
                                      DROP TABLE IF EXISTS nk.temp_object_ids;
                                      DROP TABLE IF EXISTS nk.distinct_temp_object_ids;
                                      DROP TABLE IF EXISTS nk.pub_titles;";
                conn.Execute(sql_string);
            }
        }
    }
}
