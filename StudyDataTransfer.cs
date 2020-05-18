using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class StudyDataTransfer
    {
		private string mdr_nk_connString;
		private string biolincc_ad_connString;
		private string yoda_ad_connString;
		private string ctg_ad_connString;
		private string euctr_ad_connString;
		private string isrctn_ad_connString;
		private string who_ad_connString;
		private string mon_sf_connString;

		public StudyDataTransfer()
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


        public ulong StoreStudyIds(PostgreSQLCopyHelper<StudyIds> copyHelper, IEnumerable<StudyIds> entities)
		{
			// stores the study id data in a temporary table
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
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


		public void SetUpTempStudyIdsTableInSource(int org_id)
		{
			string conn_string = GetConnString(org_id);
			using (var conn = new NpgsqlConnection(conn_string))
			{
				string sql_string = @"CREATE TABLE IF NOT EXISTS ad.temp_study_ids(
				        study_id int
                      , study_ad_id int
                      , study_sd_id varchar
                      ); ";
				conn.Execute(sql_string);
			}
		}


		public void TransferNewStudyIdsToSource(int org_id)
		{
			//using (var conn = new NpgsqlConnection(conn_string))
			//{
			//	string sql_string = @"CREATE TABLE IF NOT EXISTS ad.temp_study_ids(
			//	        study_id int
            //                   , study_ad_id int
             //                   , study_sd_id varchar
              //                   , study_hash_id varchar
                //                   ); ";
			//	conn.Execute(sql_string);
			//}

			string conn_string = GetConnString(org_id);
			using (var conn = new NpgsqlConnection(conn_string))
			{
				string sql_string = @"CREATE TABLE IF NOT EXISTS ad.temp_study_ids(
				        study_id int
                      , study_ad_id int
                      , study_sd_id varchar
                      , study_hash_id varchar
                      ); ";
				conn.Execute(sql_string);
			}
		}

		
		public void LoadStudyData(int org_id)
		{
			// Use the connection striing
			// to load the data into a List of the relevant DataBase object
			string conn_string = GetConnString(org_id);

			string sql_string = @"SELECT t.id
                        display_title, title_lang_code, brief_description, data_sharing_statement,
                        study_start_year, study_start_month, study_type_id, study_status_id,
                        study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                        max_age, max_age_units_id, " + DateTime.Now + @", 1
						FROM ad.temp_study_ids t
                        INNER JOIN ad.studies s
                        on t.ad_id = s.ad_id;";

			// Get data as a collection of objects from the soure database
			IEnumerable<StudyData> data;
			using (var conn = new NpgsqlConnection(conn_string))
			{
				data = conn.Query<StudyData>(sql_string);
			}

			// Use CopyHelper to reload it to the destination table
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				conn.Open();
				CopyHelpers.study_data_helper.SaveAll(conn, data);
			}
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


		public void DropTempStudyIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_study_ids";
				conn.Execute(sql_string);
			}
		}


	}
}
