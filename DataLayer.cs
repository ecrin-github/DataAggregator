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


		public void SetUpTempStudyIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = @"CREATE TABLE IF NOT EXISTS nk.temp_study_ids(
				        study_id int
                      , study_ad_id int
                      , study_source_id int
                      , study_sd_id varchar
                      , study_hash_id varchar
                      , datetime_of_data_fetch timestamptz
                      , is_new boolean
                      ); ";
				conn.Execute(sql_string);
			}
		}

		
		public IEnumerable<StudyIds> FetchStudyIds(int org_id)
		{
			string conn_string = GetConnString(org_id);
			using (var conn = new NpgsqlConnection(conn_string))
			{
				string sql_string = @"select ad_id as study_ad_id, " + org_id.ToString() + @" as study_source_id, 
                          sd_id as study_sd_id, hash_id as study_hash_id, datetime_of_data_fetch
                          from ad.studies
                          where record_status_id = 1";
				return conn.Query<StudyIds>(sql_string);
			}
		}


		public void CheckStudyLinks()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				// does it match where it is in the left hand column?
				string sql_string = @"UPDATE nk.temp_study_ids
		                   SET study_id = s.study_id, is_new = false
                           FROM nk.temp_study_ids t
                           INNER JOIN nk.study_study_links k
                           WHERE t.study_sd_id = k.studya_sd_id
                           AND t.study_source_id =  k.studya_source_id
                           INNER JOIN nk.all_ids_studies s
                           ON k.studyb_sd_id = s.sd_id
                           AND k.studyb_source_id = s.source_id;";

                conn.Execute(sql_string);

				// does it match where it is in the right hand column?
				sql_string = @"UPDATE nk.temp_study_ids
		                   SET study_id = s.study_id, is_new = false
                           FROM nk.temp_study_ids t
                           INNER JOIN nk.study_study_links k
                           WHERE t.study_sd_id = k.studyb_sd_id
                           AND t.study_source_id =  k.studyb_source_id
                           INNER JOIN nk.all_ids_studies s
                           ON k.studya_sd_id = s.sd_id
                           AND k.studya_source_id = s.source_id;";

				conn.Execute(sql_string);
			}
		}


		public void UpdateAllStudyIdsTable(int org_id)
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				// Add the new study id records to the all Ids table
				string sql_string = @"INSERT INTO nk.all_ids_studies
                            (study_id, study_ad_id, study_source_id, 
                             study_sd_id, datetime_of_data_fetch, is_new)
                             select study_id, study_ad_id, study_source_id, 
                             study_sd_id, datetime_of_data_fetch, is_new
                             from nk.temp_study_ids";

				conn.Execute(sql_string);

				// where the study_ids are null they can take on the value of the 
				// record id
				sql_string = @"UPDATE nk.all_ids_studies
                            SET study_id = id, is_new = true
                            WHERE study_id is null
                            AND study_source_id = " + org_id.ToString();

				conn.Execute(sql_string);

				// 'back-update' the study temp table using the newly created study_ids
				// now all should be done...
				sql_string = @"UPDATE nk.temp_study_ids t
		                   SET study_id = as.study_id, is_new = true
                           FROM nk.all_ids_studies as
                           WHERE t.study_source_id = as.study_source_id
                           AND t.study_ad_id = as.study_ad_id
                           AND t.study_id is null;";

				conn.Execute(sql_string);
			}
		}


		public void SetUpTempObjectIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = @"CREATE TABLE IF NOT EXISTS nk.temp_object_ids(
				        object_id int
                      , object_ad_id int
                      , object_source_id int
                      , object_sd_id varchar
                      , object_hash_id varchar
                      , datetime_of_data_fetch timestamptz
                      , parent_study_id int
                      , is_new boolean
                      , is_study_new boolean
                      ); ";
				conn.Execute(sql_string);
			}
		}


		public ulong StoreStudyIds(PostgreSQLCopyHelper<StudyIds> copyHelper, IEnumerable<StudyIds> entities)
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public void LoadStudyData(int org_id)
		{
			// Use the connection striing
			// to load the data into a List of the relevant DataBase object
			string conn_string = GetConnString(org_id);

			// Get data as a collection of objects from the soure database
			string sql_string = @"SELECT 

                          ;";

			IEnumerable<xxx> data = conn.Query<xxx>(sql_string);

			// Use CopyHelper to reload it to the destination table


		}


		public void LoadStudyIdentifiers(int org_id)
		{
			// if the table exists on the source connection string...
			// and it has data in it...
			string conn_string = GetConnString(org_id);
		}


		public void LoadStudyTitles(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}


		public void LoadStudyRelationShips(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}


		public void LoadStudyContributors(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}


		public void LoadStudyTopics(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}


		public void LoadStudyFeatures(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}


		public void LoadObjectData(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectDatasets(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectInstances(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectTitles(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectDates(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectContributors(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectTopics(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectRelationships(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}


	    public IEnumerable<ObjectIds> FetchObjectIds(int org_id)
		{
			string conn_string = GetConnString(org_id);
			using (var conn = new NpgsqlConnection(conn_string))
			{
				string sql_string = @"select ad_id as object_ad_id, " + org_id.ToString() + @" as object_source_id, 
                          sd_id as object_sd_id, object_hash_id, datetime_of_data_fetch
                          from ad.data_objects
                          where record_status_id = 1";

				return conn.Query<ObjectIds>(sql_string);
			}
		}


		public ulong StoreObjectIds(PostgreSQLCopyHelper<ObjectIds> copyHelper, IEnumerable<ObjectIds> entities)
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public void DropTempStudyIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_study_ids";
				conn.Execute(sql_string);
			}
		}


		public void DropTempObjectIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_object_ids";
				conn.Execute(sql_string);
			}
		}
	}


	public class StudyLink
	{
		public int source_1 { get; set; }
        public string sd_id_1 { get; set; }
		public string sd_id_2 { get; set; }
		public int source_2 { get; set; }
    }


	public class StudyIds
	{
		public int study_ad_id { get; set; }
		public int study_source_id { get; set; }
		public string study_sd_id { get; set; }
		public string study_hash_id { get; set; }
		public DateTime? datetime_of_data_fetch { get; set; }
	}


	public class ObjectIds
	{
		public int object_ad_id { get; set; }
		public int object_source_id { get; set; }
		public string object_sd_id { get; set; }
		public string object_hash_id { get; set; }
		public DateTime? datetime_of_data_fetch { get; set; }
	}


	public class CopyHelper
	{
		public PostgreSQLCopyHelper<StudyLink> links_helper =
			 new PostgreSQLCopyHelper<StudyLink>("nk", "temp_study_links_by_source")
				 .MapInteger("source_1", x => x.source_1)
				 .MapVarchar("sd_id_1", x => x.sd_id_1)
			     .MapVarchar("sd_id_2", x => x.sd_id_2)
			     .MapInteger("source_2", x => x.source_2);

		public PostgreSQLCopyHelper<StudyIds> study_ids_helper =
			 new PostgreSQLCopyHelper<StudyIds>("nk", "temp_study_ids")
				 .MapInteger("study_ad_id", x => x.study_ad_id)
				 .MapInteger("study_source_id", x => x.study_source_id)
				 .MapVarchar("study_sd_id", x => x.study_sd_id)
				 .MapVarchar("study_hash_id", x => x.study_hash_id)
				 .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);

		public PostgreSQLCopyHelper<ObjectIds> object_ids_helper =
			 new PostgreSQLCopyHelper<ObjectIds>("nk", "temp_object_ids")
				 .MapInteger("object_ad_id", x => x.object_ad_id)
				 .MapInteger("object_source_id", x => x.object_source_id)
				 .MapVarchar("object_sd_id", x => x.object_sd_id)
				 .MapVarchar("object_hash_id", x => x.object_hash_id)
				 .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);
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

}
