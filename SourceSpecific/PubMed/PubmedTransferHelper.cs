using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

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

		public string SetUpTempContextFTW()
		{
			using (var conn = new NpgsqlConnection(pubmed_connString))
			{
				string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
			                         schema ad;";
				conn.Execute(sql_string);

				sql_string = @"CREATE SERVER IF NOT EXISTS context
						      FOREIGN DATA WRAPPER postgres_fdw
                              OPTIONS (host 'localhost', dbname 'context');";
				conn.Execute(sql_string);

				sql_string = @"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                     SERVER context
					 OPTIONS (user '" + user + "', password '" + password + "');";
				conn.Execute(sql_string);

				string schema_name = "context_ctx";
				sql_string = @"DROP SCHEMA IF EXISTS " + schema_name + @" cascade;
                     CREATE SCHEMA " + schema_name + @";
                     IMPORT FOREIGN SCHEMA ctx
                     FROM SERVER context
					 INTO " + schema_name + ";";
				conn.Execute(sql_string);

				return schema_name;
			}
		}


		public void DropTempContextFTW()
		{
			using (var conn = new NpgsqlConnection(pubmed_connString))
			{
				string schema_name = "context_ctx";

				string sql_string = @"DROP USER MAPPING IF EXISTS FOR CURRENT_USER
                     SERVER " + schema_name + ";";
				conn.Execute(sql_string);

				sql_string = @"DROP SERVER IF EXISTS context CASCADE;";
				conn.Execute(sql_string);

				sql_string = @"DROP SCHEMA IF EXISTS " + schema_name;
				conn.Execute(sql_string);
			}
		}


		public IEnumerable<PMIDLink> FetchBankPMIDs()
		{
			using (var conn = new NpgsqlConnection(pubmed_connString))
			{
				string sql_string = @"select 
                        100135 as source_id, 
                        d.id as parent_study_source_id, 
                        k.sd_oid, k.id_in_db as parent_study_sd_sid, 
                        a.datetime_of_data_fetch
                        from ad.object_db_links k
                        inner join ad.data_objects a 
                        on k.sd_oid = a.sd_oid
                        inner join context_ctx.nlm_databanks d
                        on k.db_name = d.nlm_abbrev";
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
				string sql_string = @"INSERT INTO nk.distinct_pmids(
						 source_id, sd_oid, parent_study_source_id, 
				         parent_study_sd_sid)
                         SELECT distinct 
                         source_id, sd_oid, parent_study_source_id, 
				         parent_study_sd_sid
                         FROM nk.temp_pmids;";
				conn.Execute(sql_string);

				// update with latest datetime_of_data_fetch
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
