using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class StudyDataTransferrer
    {
		DataLayer repo;
		string mdr_connString;

		public StudyDataTransferrer(DataLayer _repo)
		{
			repo = _repo;
			mdr_connString = repo.GetMDRConnString();
		}


		public void SetUpTempStudyIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
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
			string conn_string = repo.GetConnString(org_id);
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
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public void CheckStudyLinks()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				// does it match a study in the link table
				// that is the left hand column - which is 
				// the one to be replaced if necessaary
				string sql_string = @"UPDATE nk.temp_study_ids
		                   SET study_id = s.study_id, is_new = false
                           FROM nk.temp_study_ids t
                           INNER JOIN nk.study_study_links k
                           WHERE t.study_sd_id = k.sd_id
                           AND t.study_source_id =  k.source_id
                           INNER JOIN nk.all_ids_studies s
                           ON k.preferred_sd_id = s.sd_id
                           AND k.preferred_source_id = s.source_id;";

				conn.Execute(sql_string);
			}
		}


		public void UpdateAllStudyIdsTable(int org_id)
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
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


		public string SetUpTempFTW(string database_name)
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string schema_name = database_name + "_ad";
				string sql_string = @"CREATE SCHEMA IF NOT EXISTS " + schema_name;
				conn.Execute(sql_string);

				sql_string = @"CREATE SERVER IF NOT EXISTS " + database_name
						   + @" FOREIGN DATA WRAPPER postgres_fdw
                             OPTIONS (host 'localhost', dbname '" + database_name + "');";
                conn.Execute(sql_string);

				sql_string = @"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                     SERVER " + database_name
					 + @" OPTIONS (user '" + repo.GetUserName() + "', password '" + repo.GetPassword() + "');";
				conn.Execute(sql_string);

				sql_string = @"IMPORT FOREIGN SCHEMA ad
                     FROM SERVER " + database_name + 
					 @" INTO " + schema_name + ";";
				conn.Execute(sql_string);

				return schema_name;
			}
		}
		
		public void LoadStudyData(string schema_name)
		{
			string sql_string = @"SELECT t.id
                        s.display_title, s.title_lang_code, s.brief_description, s.data_sharing_statement,
                        s.study_start_year, s.study_start_month, s.study_type_id, s.study_status_id,
                        s.study_enrolment, s.study_gender_elig_id, s.min_age, s.min_age_units_id,
                        s.max_age, s.max_age_units_id, " + DateTime.Now + @", 1
						FROM ad.temp_study_ids t
                        INNER JOIN " + schema_name + @".studies s
                        on t.ad_id = s.ad_id;";

			// Get data as a collection of objects from the soure database
			IEnumerable<StudyData> data;
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				conn.Execute(sql_string);
			}
		}

		public void LoadStudyIdentifiers(string schema_name)
		{

		}


		public void LoadStudyTitles(string schema_name)
		{

		}


		public void LoadStudyRelationShips(string schema_name)
		{

		}


		public void LoadStudyContributors(string schema_name)
		{

		}


		public void LoadStudyTopics(string schema_name)
		{

		}


		public void LoadStudyFeatures(string schema_name)
		{

		}


		public void DropTempStudyIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_study_ids";
				conn.Execute(sql_string);
			}
		}


		public void DropTempFTW(string database_name)
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string schema_name = database_name + "_ad";

				string sql_string = @"DROP USER MAPPING IF EXISTS FOR CURRENT_USER
                     SERVER " + schema_name + ";";
                conn.Execute(sql_string);

                sql_string = @"DROP SERVER IF EXISTS " + database_name + " CASCADE;";
				conn.Execute(sql_string);

				sql_string = @"DROP SCHEMA IF EXISTS " + schema_name;
				conn.Execute(sql_string);
			}
		}

	}
}
