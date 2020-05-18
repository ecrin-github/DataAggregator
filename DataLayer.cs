using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using PostgreSQLCopyHelper;

namespace DataAggregator
{
	public class DataLayer
	{
		private string mdr_nk_connString;
		private string biolincc_ad_connString;
		private string yoda_ad_connString;
		private string ctg_ad_connString;
		private string euctr_ad_connString;
		private string isrctn_ad_connString;
		private string who_ad_connString;
		private string mon_sf_connString;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// The json file also includes the root folder path, which is
		/// stored in the class's folder_base property.
		/// </summary>
		public DataLayer()
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
			builder.SearchPath = "nk";
			mdr_nk_connString = builder.ConnectionString;

			builder.Database = "biolincc";
			builder.SearchPath = "ad";
			biolincc_ad_connString = builder.ConnectionString;

			builder.Database = "ctg";
			builder.SearchPath = "ad";
			ctg_ad_connString = builder.ConnectionString;

			builder.Database = "yoda";
			builder.SearchPath = "ad";
			yoda_ad_connString = builder.ConnectionString;

			builder.Database = "euctr";
			builder.SearchPath = "ad";
			euctr_ad_connString = builder.ConnectionString;

			builder.Database = "isrctn";
			builder.SearchPath = "ad";
			isrctn_ad_connString = builder.ConnectionString;

			builder.Database = "who";
			builder.SearchPath = "ad";
			who_ad_connString = builder.ConnectionString;

			builder.Database = "mon";
			builder.SearchPath = "sf";
			mon_sf_connString = builder.ConnectionString;


			// example appsettings.json file...
			// the only values required are for...
			// {
			//	  "host": "host_name...",
			//	  "user": "user_name...",
			//    "password": "user_password...",
			//	  "folder_base": "C:\\MDR JSON\\Object JSON... "
			// }
		}


		public IEnumerable<FileRecord> FetchStudyFileRecords(int source_id)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(mon_sf_connString))
			{
				string sql_string = "select id, source_id, sd_id, remote_url, last_sf_id, last_revised, ";
				sql_string += " assume_complete, download_status, download_datetime, local_path, last_processed ";
				sql_string += " from sf.source_data_studies ";
				sql_string += " where source_id = " + source_id.ToString();
				sql_string += " and local_path is not null";
				sql_string += " order by local_path";
				return Conn.Query<FileRecord>(sql_string);
			}
		}


		// get record of interest
		public FileRecord FetchStudyFileRecord(string sd_id, int source_id)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(mon_sf_connString))
			{
				string sql_string = "select id, source_id, sd_id, remote_url, last_sf_id, last_revised, ";
				sql_string += " assume_complete, download_status, download_datetime, local_path, last_processed ";
				sql_string += " from sf.source_data_studies ";
				sql_string += " where sd_id = '" + sd_id + "' and source_id = " + source_id.ToString();
				return Conn.Query<FileRecord>(sql_string).FirstOrDefault();
			}
		}

		public bool StoreStudyFileRec(FileRecord file_record)
		{
			using (var conn = new NpgsqlConnection(mon_sf_connString))
			{
				return conn.Update<FileRecord>(file_record);
			}
		}


		public int InsertStudyFileRec(FileRecord file_record)
		{
			using (var conn = new NpgsqlConnection(mon_sf_connString))
			{
				return (int)conn.Insert<FileRecord>(file_record);
			}
		}


		public void UpdateStudyFileRecLastProcessed(int id)
		{
			using (var conn = new NpgsqlConnection(mon_sf_connString))
			{
				string sql_string = "update sf.source_data_studies";
				sql_string += " set last_processed = current_timestamp";
				sql_string += " where id = " + id.ToString();
				conn.Execute(sql_string);
			}
		}

		public string GetConnString(int org_id)
		{
			string conn_string = "";
			switch (org_id)
			{
				case 100120: { conn_string = ctg_ad_connString; break; }
				case 100123: { conn_string = euctr_ad_connString; break; }
				case 100126: { conn_string = isrctn_ad_connString; break; }
				case 100900: { conn_string = biolincc_ad_connString; break; }
				case 100901: { conn_string = yoda_ad_connString; break; }
				case 100115: { conn_string = who_ad_connString; break; }
			}
			return conn_string;
		}

			
		public IEnumerable<StudyLink> FetchLinks(int org_id)
		{
			string conn_string = GetConnString(org_id);
			using (var conn = new NpgsqlConnection(conn_string))
			{
				string sql_string;
				if (org_id != 100115)
				{
					sql_string = @"select " + org_id.ToString() + @" as source_1, sd_id as sd_id_1, 
                          identifier_value as sd_id_2, identifier_org_id as source_2
                          from ad.study_identifiers
                          where identifier_type_id = 11
                          and identifier_org_id <> 100115
                          and identifier_org_id <> " + org_id.ToString();
				}
				else
				{
					sql_string = @"select source_1, sd_id_1, sd_id_2, source_2
                          from ad.temp_who_ids;";
				}
				return conn.Query<StudyLink>(sql_string);
			}
		}


		public void SetUpTempLinksBySourceTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = @"CREATE TABLE IF NOT EXISTS nk.temp_study_links_by_source(
				        source_1 int
                      , sd_id_1 varchar
                      , sd_id_2 varchar
                      , source_2 int) ";
				conn.Execute(sql_string);
			}
		}

		public void TruncateLinksBySourceTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{ 
				string sql_string = @"TRUNCATE TABLE nk.temp_study_links_by_source";
				conn.Execute(sql_string);
			}
		}

		public void SetUpTempLinkCollectorTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = @"CREATE TABLE IF NOT EXISTS nk.temp_study_links_collector(
				        source_1 int
                      , sd_id_1 varchar
                      , sd_id_2 varchar
                      , source_2 int) ";
				conn.Execute(sql_string);
			}
		}

		public void TransferLinksToCollectorTable(int source_id)
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				// needs to be done twice to keep the ordering of aouerces correct
				
				string sql_string = @"INSERT INTO nk.temp_study_links_collector(
				          source_1, sd_id_1, sd_id_2, source_2) 
                          SELECT source_1, sd_id_1, sd_id_2, source_2
						  FROM nk.temp_study_links_by_source t
                          WHERE source_1 > source_2";

				conn.Execute(sql_string);

				sql_string = @"INSERT INTO nk.temp_study_links_collector(
				          source_1, sd_id_1, sd_id_2, source_2) 
                          SELECT source_2, sd_id_2, sd_id_1, source_1
						  FROM nk.temp_study_links_by_source t
                          WHERE source_1 < source_2";

				conn.Execute(sql_string);
			}
		}


		public void TransferNewLinksToDataTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = @"TRUNCATE TABLE nk.study_study_links;
                          INSERT INTO nk.study_study_links(
				          studya_source_id, studya_sd_id, studyb_sd_id, studyb_source_id) 
                          SELECT distinct source_1, sd_id_1, sd_id_2, source_2
						  FROM nk.temp_study_links_collector";

				conn.Execute(sql_string);
			}
		}


		public int ObtainTotalOfNewLinks()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = @"SELECT COUNT(*) FROM nk.temp_study_links_collector";
				return conn.ExecuteScalar<int>(sql_string);
			}
		}


		public void DropTempLinksBySourceTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_study_links_by_source";
				conn.Execute(sql_string);
			}
		}

		public void DropTempLinkCollectorTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_study_links_collector";
				conn.Execute(sql_string);
			}
		}

		public ulong StoreLinks(PostgreSQLCopyHelper<StudyLink> copyHelper, IEnumerable<StudyLink> entities)
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		// Returns the total number of PubMed Ids to be processd

		public int GetSourceRecordCount()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string query_string = @"SELECT COUNT(*) FROM sf.source_data_objects 
                                WHERE source_id = 100135 AND download_status = 0";
				return conn.ExecuteScalar<int>(query_string);
			}
		}

	}


}
