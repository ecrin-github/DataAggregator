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
                      , is_preferred boolean
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
		                   SET study_id = s.study_id, is_preferred = false
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
                             study_sd_id, datetime_of_data_fetch, is_preferred)
                             select study_id, study_ad_id, study_source_id, 
                             study_sd_id, datetime_of_data_fetch, is_preferred
                             from nk.temp_study_ids";

				conn.Execute(sql_string);

				// Where the study_ids are null they can take on the value of the 
				// record id.

				sql_string = @"UPDATE nk.all_ids_studies
                            SET study_id = id, is_preferred = true
                            WHERE study_id is null
                            AND study_source_id = " + org_id.ToString();

				conn.Execute(sql_string);

				// 'Back-update' the study temp table using the newly created study_ids
				// now all should be done...

				sql_string = @"UPDATE nk.temp_study_ids t
		                   SET study_id = as.study_id, is_preferred = true
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
		
		public void LoadNewStudyData(string schema_name)
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = @"INSERT INTO st.studies(id, 
                        display_title, title_lang_code, brief_description, data_sharing_statement,
                        study_start_year, study_start_month, study_type_id, study_status_id,
                        study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                        max_age, max_age_units_id, date_of_data, record_status_id)
                        SELECT t.id
                        s.display_title, s.title_lang_code, s.brief_description, s.data_sharing_statement,
                        s.study_start_year, s.study_start_month, s.study_type_id, s.study_status_id,
                        s.study_enrolment, s.study_gender_elig_id, s.min_age, s.min_age_units_id,
                        s.max_age, s.max_age_units_id, " + DateTime.Now + @", s.record_status_id
						FROM ad.temp_study_ids t
                        INNER JOIN " + schema_name + @".studies s
                        on t.ad_id = s.ad_id
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = true;";

				conn.Execute(sql_string);

				// Note that this also updates studies that are not added as new
				// (because they equate to existing studies) but which were new in the 
				// source data

				sql_string = @"UPDATE " + schema_name + @".studies s
                        SET record_status_id = 5
                        WHERE s.record_status_id = 1;";

				conn.Execute(sql_string);
			}
		}


		public void LoadNewStudyIdentifiers(string schema_name)
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				// Note may apply to new identifiers of existing studies as well as 
				// identifiers attached to new studies

				// action should depend on whether study id / source is the 'preferred ' for this study
				string sql_string = @"INSERT INTO st.study_identifiers(study_id, 
                        identifier_type_id, identifier_org_id, identifier_org, 
                        identifier_value, identifier_date, identifier_link,
                        date_of_data, record_status_id)

                        SELECT t.id
                        s.identifier_type_id, s.identifier_org_id, s.identifier_org, 
                        s.identifier_value, s.identifier_date, s.identifier_link,
                        " + DateTime.Now + @", s.record_status_id
						FROM ad.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_identifiers s
                        on t.ad_id = s.study_ad_id
                        WHERE s.record_status_id = 1
                        AND t.is_preferred= true;";

				conn.Execute(sql_string);

				sql_string = @"INSERT INTO st.study_identifiers(study_id, 
                        identifier_type_id, identifier_org_id, identifier_org, 
                        identifier_value, identifier_date, identifier_link,
                        date_of_data, record_status_id)

                        SELECT t.id
                        s.identifier_type_id, s.identifier_org_id, s.identifier_org, 
                        s.identifier_value, s.identifier_date, s.identifier_link,
                        " + DateTime.Now + @", s.record_status_id
						FROM ad.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_identifiers s
                        on t.ad_id = s.study_ad_id
                        LEFT JOIN st.study_identifiers s2
                        ON s.study_id = t.study_id
                        AND s.identifier_type_id = s2.identifier_type_id
                        AND s.identifier_value = s2.identifier_value
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = false 
                        AND s2.study_id is null;";

				conn.Execute(sql_string);

				// Update status of records

				sql_string = @"UPDATE " + schema_name + @".study_identifiers s
                        SET record_status_id = 5
                        WHERE s.record_status_id = 1;";

				conn.Execute(sql_string);
			}
		}


		public void LoadNewStudyTitles(string schema_name)
		{

		}


		public void LoadNewStudyRelationShips(string schema_name)
		{

		}


		public void LoadNewStudyContributors(string schema_name)
		{

		}


		public void LoadNewStudyTopics(string schema_name)
		{

		}


		public void LoadNewStudyFeatures(string schema_name)
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
