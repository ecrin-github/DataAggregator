using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;

namespace DataAggregator
{
    class PubmedTransferHelper
    {
        NpgsqlConnectionStringBuilder builder;
        private string connString;
        private string pubmed_connString;
        private string user;
        private string password;

        /// <summary>
        /// Parameterless constructor is used to automatically build
        /// the connection string, using an appsettings.json file that 
        /// has the relevant credentials (but which is not stored in GitHub).
        /// The json file also includes the root folder path, which is
        /// stored in the class's folder_base property.
        /// </summary>
        /// 
        public PubmedTransferHelper()
        {
            IConfigurationRoot settings = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            builder = new NpgsqlConnectionStringBuilder();
            builder.Host = settings["host"];
            builder.Username = settings["user"];
            builder.Password = settings["password"];
            builder.Database = "mdr";
            
            user = settings["user"];
            password = settings["password"];

            connString = builder.ConnectionString;

            builder.Database = "pubmed";
            pubmed_connString = builder.ConnectionString;
        }

        // Tables and functions used for the PMIDs collected from DB Sources

        public void SetupTempPMIDTable()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"DROP TABLE IF EXISTS nk.temp_pmids;
                      CREATE TABLE IF NOT EXISTS nk.temp_pmids(
                        source_id                INT
                      , sd_oid                   VARCHAR
                      , parent_study_source_id   INT 
                      , parent_study_sd_sid      VARCHAR
                      , datetime_of_data_fetch   TIMESTAMPTZ
                      ); ";
                conn.Execute(sql_string);
            }
        }

        public void SetupDistinctPMIDTable()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"DROP TABLE IF EXISTS nk.distinct_pmids;
                      CREATE TABLE IF NOT EXISTS nk.distinct_pmids(
                        source_id                INT
                      , sd_oid                   VARCHAR
                      , parent_study_source_id   INT 
                      , parent_study_sd_sid      VARCHAR
                      , datetime_of_data_fetch   TIMESTAMPTZ
                      ); ";
                conn.Execute(sql_string);
            }
        }


        public IEnumerable<PMIDLink> FetchBankPMIDs()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"select 
                        100135 as source_id, 
                        d.id as parent_study_source_id, 
                        k.sd_oid, k.id_in_db as parent_study_sd_sid, 
                        a.datetime_of_data_fetch
                        from pubmed_ad.object_db_links k
                        inner join pubmed_ad.data_objects a 
                        on k.sd_oid = a.sd_oid
                        inner join context_ctx.nlm_databanks d
                        on k.db_name = d.nlm_abbrev
                        where bank_type <> 'databank'";
                return conn.Query<PMIDLink>(sql_string);
            }
        }


        public ulong StorePMIDLinks(PostgreSQLCopyHelper<PMIDLink> copyHelper, IEnumerable<PMIDLink> entities)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public IEnumerable<PMIDLink> FetchSourceReferences(int source_id, string db_name)
        {
            builder.Database = db_name;
            string db_conn_string = builder.ConnectionString;

            using (var conn = new NpgsqlConnection(db_conn_string))
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


        public void FillDistinctPMIDsTable()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                // First ensure that any PMIDs (sd_oids) are in the same format
                // Some have a 'tail' of spaces after them, as the standard 
                // length of a sd_oid is 24 characters.
                
                string sql_string = @"UPDATE nk.temp_pmids
                         SET sd_oid = trim(sd_oid);";
                conn.Execute(sql_string);

                // Then transfer the distinct data

                sql_string = @"INSERT INTO nk.distinct_pmids(
                         source_id, sd_oid, parent_study_source_id, 
                         parent_study_sd_sid)
                         SELECT distinct 
                         source_id, sd_oid, parent_study_source_id, 
                         parent_study_sd_sid
                         FROM nk.temp_pmids;";
                conn.Execute(sql_string);

                // Update with latest datetime_of_data_fetch

                sql_string = @"UPDATE nk.distinct_pmids dp
                         set datetime_of_data_fetch = mx.max_fetch_date
                         FROM 
                         ( select sd_oid, parent_study_sd_sid, 
                           max(datetime_of_data_fetch) as max_fetch_date
                           FROM nk.temp_pmids
                           group by sd_oid, parent_study_sd_sid ) mx
                         WHERE dp.parent_study_sd_sid = mx.parent_study_sd_sid
                         and dp.sd_oid = mx.sd_oid;";
                conn.Execute(sql_string);
            }
        }


        public void CleanPMIDsdsidData1()
        {
            string sql_string = "";
            using (var conn = new NpgsqlConnection(connString))
            {
                sql_string = @"UPDATE nk.distinct_pmids
                        SET parent_study_sd_sid = 'ACTRN' || parent_study_sd_sid
                        WHERE parent_study_source_id = 100116
                        AND length(parent_study_sd_sid) = 14;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, ' ', '')
                        WHERE parent_study_source_id = 100116;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, '#', '')
                        WHERE parent_study_source_id = 100116;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, ':', '')
                        WHERE parent_study_source_id = 100116;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, '[', '')
                        WHERE parent_study_source_id = 100116;";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.distinct_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'CHICTR', 'ChiCTR')
                        WHERE parent_study_source_id = 100118;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                        SET parent_study_sd_sid = 'ChiCTR-' || parent_study_sd_sid
                        WHERE parent_study_source_id = 100118
                        and parent_study_sd_sid not ilike 'ChiCTR-%';";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                        SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'ChiCTR-ChiCTR', 'ChiCTR-')
                        WHERE parent_study_source_id = 100118;";
                conn.Execute(sql_string);
            }
        }


        public void CleanPMIDsdsidData2()
        {
            string sql_string = "";
            using (var conn = new NpgsqlConnection(connString))
            {
                sql_string = @"UPDATE nk.distinct_pmids
                     SET parent_study_sd_sid = Replace(parent_study_sd_sid, '/', '-')
                     WHERE parent_study_source_id = 100121;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                     SET parent_study_sd_sid = 'CTRI-' || parent_study_sd_sid
                     WHERE parent_study_source_id = 100121
                     and parent_study_sd_sid not ilike 'CTRI-%';";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                     SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'REF-', '')
                     WHERE parent_study_source_id = 100121;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                     SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'CTRI-CTRI', 'CTRI-')
                     WHERE parent_study_source_id = 100121;";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = 'RPCEC' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100122
                   and parent_study_sd_sid not ilike 'RPCEC%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = UPPER(parent_study_sd_sid)
                   WHERE parent_study_source_id = 100123;"; 
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '–', '-')
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EUDRA-CT', 'EUDRACT')
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EUDRACT', '')
                    WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EURODRACT', '')
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'EU', '')
                   WHERE parent_study_source_id = 100123;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'CT', '')
                   WHERE parent_study_source_id = 100123;";

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = left(parent_study_sd_sid, 14)
                   WHERE parent_study_source_id = 100123
                   and length(parent_study_sd_sid) > 14;";
                conn.Execute(sql_string);
            }
        }


        public void CleanPMIDsdsidData3()
        {
            string sql_string = "";
            using (var conn = new NpgsqlConnection(connString))
            {
                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = Replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100124;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = Replace(parent_study_sd_sid, '-', '')
                   WHERE parent_study_source_id = 100124;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'DKRS', 'DRKS')
                   WHERE parent_study_source_id = 100124;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = Replace(parent_study_sd_sid, 'DRK0', 'DRKS0')
                   WHERE parent_study_source_id = 100124;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = 'DRKS' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100124
                   and parent_study_sd_sid not ilike 'DRKS%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = 'IRCT' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100125
                   and parent_study_sd_sid not ilike 'IRCT%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'SRCTN', 'ISRCTN')
                   WHERE parent_study_source_id = 100126
                   and parent_study_sd_sid ilike 'SRCTN%';";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'ISRTN', 'ISRCTN')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'ISRNT', 'ISRCTN')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = 'ISRCTN' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100126
                   and parent_study_sd_sid not ilike 'ISRCTN%';";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '#', '')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '/', '')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ':', '')
                   WHERE parent_study_source_id = 100126;";
                conn.Execute(sql_string);

            }
        }


        public void CleanPMIDsdsidData4()
        {
            string sql_string = "";
            using (var conn = new NpgsqlConnection(connString))
            {
                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100128;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'PACTCR', 'PACTR')
                   WHERE parent_study_source_id = 100128;";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = 'PACTR' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100128
                   and parent_study_sd_sid not ilike 'PACTR%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '/', '-')
                   WHERE parent_study_source_id = 100130;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = 'SLCTR-' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100130
                   and parent_study_sd_sid not ilike 'SLCTR-%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '-', '')
                   WHERE parent_study_source_id = 100131;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100131;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = 'TCTR' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100131
                   and parent_study_sd_sid not ilike 'TCTR%';";
                conn.Execute(sql_string);


                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, 'NTRR', 'NTR')
                   WHERE parent_study_source_id = 100132;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, ' ', '')
                   WHERE parent_study_source_id = 100132;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = replace(parent_study_sd_sid, '-', '')
                   WHERE parent_study_source_id = 100132;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.distinct_pmids
                   SET parent_study_sd_sid = 'NTR' || parent_study_sd_sid
                   WHERE parent_study_source_id = 100132
                   and parent_study_sd_sid not ilike 'NTR%'
                   and parent_study_sd_sid not ilike 'NL%';";
                conn.Execute(sql_string);

            }
        }


        public void TransferPMIDLinksToObjectIds()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"INSERT INTO nk.temp_object_ids(
                         source_id, sd_oid, parent_study_source_id, 
                         parent_study_sd_sid, datetime_of_data_fetch)
                         SELECT  
                         source_id, sd_oid, parent_study_source_id, 
                         parent_study_sd_sid, datetime_of_data_fetch
                         FROM nk.distinct_pmids";
                conn.Execute(sql_string);
            }
        }


        public void InputPreferredSDSIDS()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                // replace any LHS sd_sids with the 'preferred' RHS

                string sql_string = @"UPDATE nk.temp_object_ids b
                               SET parent_study_sd_sid = preferred_sd_sid,
                               parent_study_source_id = preferred_source_id
                               FROM nk.study_study_links k
                               WHERE b.parent_study_sd_sid = k.sd_sid
                               and b.parent_study_source_id = k.source_id ;";
                conn.Execute(sql_string);

                // That may have produced some duplicates - if so get rid of them
                // needs to be done indirectly because of the need to get the maximum
                // datetime_of_data_fetch for each duplciated object

                sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids2;
                CREATE TABLE IF NOT EXISTS nk.temp_object_ids2(
                  object_id                INT
                , source_id                INT
                , sd_oid                   VARCHAR
                , parent_study_source_id   INT
                , parent_study_sd_sid      VARCHAR
                , parent_study_id          INT
                , is_preferred_study       BOOLEAN  default true
                , datetime_of_data_fetch   TIMESTAMPTZ
                ); ";
                conn.Execute(sql_string);

                sql_string = @"INSERT INTO nk.temp_object_ids2(
                         source_id, sd_oid, parent_study_source_id, 
                         parent_study_sd_sid, parent_study_id)
                         SELECT distinct 
                         source_id, sd_oid, parent_study_source_id, 
                         parent_study_sd_sid, parent_study_id
                         FROM nk.temp_object_ids;";
                conn.Execute(sql_string);

                // update with latest datetime_of_data_fetch
                // for each distinct study - data object link 

                sql_string = @"UPDATE nk.temp_object_ids2 to2
                         set datetime_of_data_fetch = mx.max_fetch_date
                         FROM 
                         ( select sd_oid, parent_study_sd_sid,
                           max(datetime_of_data_fetch) as max_fetch_date
                           FROM nk.temp_object_ids
                           group by sd_oid, parent_study_sd_sid ) mx
                         WHERE to2.sd_oid = mx.sd_oid
                         and to2.parent_study_sd_sid = mx.parent_study_sd_sid;";
                conn.Execute(sql_string);

                sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
                ALTER TABLE nk.temp_object_ids2 RENAME TO temp_object_ids;";
                conn.Execute(sql_string);

                // Maybe a few blank pmids slip through...

                sql_string = @"delete from nk.temp_object_ids
                    where sd_oid is null or sd_oid = '';";
                conn.Execute(sql_string);
            }
        }


        public void ResetIdsOfDuplicatedPMIDs()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                // Find the minimum object_id for each PMID in the table
                // source id for PubMed = 100135

                string sql_string = @"DROP TABLE IF EXISTS nk.temp_min_object_ids;
                                     CREATE TABLE nk.temp_min_object_ids as
                                     SELECT sd_oid, Min(id) as min_id
                                     FROM nk.all_ids_data_objects
                                     WHERE source_id = 100135
                                     GROUP BY sd_oid;";
                conn.Execute(sql_string);

                sql_string = @"UPDATE nk.all_ids_data_objects b
                               SET object_id = min_id
                               FROM nk.temp_min_object_ids m
                               WHERE b.sd_oid = m.sd_oid
                               and source_id = 100135;";
                conn.Execute(sql_string);

                sql_string = @"DROP TABLE nk.temp_min_object_ids;";
                conn.Execute(sql_string);

                // ???? May be (yet) more duplicates have appeared, where a 
                // study - pmid link has been generated in more than one way
                // (Links of the same paper to different studies are not uncommon)

            }
        }


        public void DropTempPMIDTable()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = "DROP TABLE IF EXISTS pp.temp_pmids";
                conn.Execute(sql_string);
            }
        }


        

    }
}
