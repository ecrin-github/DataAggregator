using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class CoreDataTransferrer
    {
		DataLayer repo;
		string connString;

		public CoreDataTransferrer(DataLayer _repo)
		{
			repo = _repo;
			connString = repo.ConnString;
		}


		public void SetUpTempStudyIdsTable()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_ids;
                        CREATE TABLE nk.temp_study_ids(
				        study_id int
                      , source_id int
                      , sd_sid varchar
                      , datetime_of_data_fetch timestamptz
                      , is_preferred boolean
                      ); ";
				conn.Execute(sql_string);
			}
		}

        public IEnumerable<StudyId> FetchStudyIds(int source_id)
        {
            string conn_string = repo.GetConnString(source_id);
            using (var conn = new NpgsqlConnection(conn_string))
            {
                string sql_string = @"select " + source_id.ToString() + @" as source_id, 
                          sd_id as study_sd_id, datetime_of_data_fetch
                          from ad.studies";

                return conn.Query<StudyId>(sql_string);
            }
        }


        public ulong StoreStudyIds(PostgreSQLCopyHelper<StudyId> copyHelper, IEnumerable<StudyId> entities)
		{
			// stores the study id data in a temporary table
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public void CheckStudyLinks()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// does it match a study in the link table
				// that is the left hand column - which is 
				// the one to be replaced if necessaary
				string sql_string = @"UPDATE nk.temp_study_ids t
		                   SET study_id = s.study_id, is_preferred = false
                           FROM nk.study_study_links k
                                INNER JOIN nk.all_ids_studies s
                                ON k.preferred_sd_id = s.study_sd_id
                                AND k.preferred_source_id = s.study_source_id
                           WHERE t.study_sd_id = k.sd_id
                           AND t.study_source_id =  k.source_id;";
				conn.Execute(sql_string);
			}
		}


		public void UpdateAllStudyIdsTable(int org_id)
		{
			using (var conn = new NpgsqlConnection(connString))
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
		                   SET study_id = a.study_id, is_preferred = true
                           FROM nk.all_ids_studies a
                           WHERE t.study_source_id = a.study_source_id
                           AND t.study_ad_id = a.study_ad_id
                           AND t.study_id is null;";

				conn.Execute(sql_string);
			}
		}


		
		public void LoadNewStudyData(string schema_name)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"INSERT INTO st.studies(id, 
                        display_title, title_lang_code, brief_description, data_sharing_statement,
                        study_start_year, study_start_month, study_type_id, study_status_id,
                        study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                        max_age, max_age_units_id, date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.display_title, s.title_lang_code, s.brief_description, s.data_sharing_statement,
                        s.study_start_year, s.study_start_month, s.study_type_id, s.study_status_id,
                        s.study_enrolment, s.study_gender_elig_id, s.min_age, s.min_age_units_id,
                        s.max_age, s.max_age_units_id, current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".studies s
                        on t.study_sd_id = s.sd_id
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
			using (var conn = new NpgsqlConnection(connString))
			{
				// Note may apply to new identifiers of existing studies as well as 
				// identifiers attached to new studies

				// action should depend on whether study id / source is the 'preferred ' for this study
				string sql_string = @"INSERT INTO st.study_identifiers(study_id, 
                        identifier_type_id, identifier_org_id, identifier_org, 
                        identifier_value, identifier_date, identifier_link,
                        date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.identifier_type_id, s.identifier_org_id, s.identifier_org, 
                        s.identifier_value, s.identifier_date, s.identifier_link,
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_identifiers s
                        on t.study_sd_id = s.sd_id
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = true;";

				conn.Execute(sql_string);

				sql_string = @"INSERT INTO st.study_identifiers(study_id, 
                        identifier_type_id, identifier_org_id, identifier_org, 
                        identifier_value, identifier_date, identifier_link,
                        date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.identifier_type_id, s.identifier_org_id, s.identifier_org, 
                        s.identifier_value, s.identifier_date, s.identifier_link,
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_identifiers s
                        on t.study_sd_id = s.sd_id
                        LEFT JOIN st.study_identifiers s2
                        ON s2.study_id = t.study_id
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
			using (var conn = new NpgsqlConnection(connString))
			{
				// action should depend on whether study id / source is the 'preferred ' for this study
				string sql_string = @"INSERT INTO st.study_titles(study_id, 
                        title_type_id, title_text, title_lang_code, 
                        lang_usage_id, is_default, contains_html,
                        comparison_text, comments, date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.title_type_id, s.title_text, s.title_lang_code, 
                        s.lang_usage_id, s.is_default, s.contains_html,
                        s.comparison_text, s.comments, 
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_titles s
                        on t.study_sd_id = s.sd_id
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = true;";

				conn.Execute(sql_string);

				// with existing studies sources all titles should be non default
				// only titles not already in the system need to be added

				sql_string = @"INSERT INTO st.study_titles(study_id, 
                        title_type_id, title_text, title_lang_code, 
                        lang_usage_id, is_default, contains_html,
                        comparison_text, comments, date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.title_type_id, s.title_text, s.title_lang_code, 
                        s.lang_usage_id, false, s.contains_html,
                        s.comparison_text, s.comments, 
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_titles s
                        on t.study_sd_id = s.sd_id
                        LEFT JOIN st.study_titles s2
                        ON s2.study_id = t.study_id
                        AND s.title_type_id = s2.title_type_id
                        AND s.title_text = s2.title_text
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = false 
                        AND s2.study_id is null;";

				conn.Execute(sql_string);

				// Update status of records

				sql_string = @"UPDATE " + schema_name + @".study_titles s
                        SET record_status_id = 5
                        WHERE s.record_status_id = 1;";

				conn.Execute(sql_string);
			}
		}


		public void LoadNewStudyRelationShips(string schema_name)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// action should depend on whether study id / source is the 'preferred ' for this study

				string sql_string = @"INSERT INTO st.study_relationships(study_id, 
                        sd_id, relationship_id, target_sd_id, comments,
                        date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.sd_id, s.relationship_id, s.target_sd_id, s.comments,
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_titles s
                        on t.study_sd_id = s.sd_id
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = true;";

				conn.Execute(sql_string);

				// with existing studies sources relationshipsd are added
				// only if they do not already exist

				sql_string = @"INSERT INTO st.study_relationships(study_id, 
                        sd_id, relationship_id, target_sd_id, comments,
                        date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.sd_id, s.relationship_id, s.target_sd_id, s.comments,
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_relationships s
                        on t.study_sd_id = s.sd_id
                        LEFT JOIN st.study_titles s2
                        ON s2.study_id = t.study_id
                        AND s.relationship_id = s2.relationship_id
                        AND s.target_sd_id = s2.target_sd_id
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = false 
                        AND s2.study_id is null;";

				conn.Execute(sql_string);

                // insert target study id, using sd_id to find it in the temp studies table
                // N.B. These relationships are defined within the same source...
                // (Cross source relationships are defined in the links (nk) schema

                sql_string = @"UPDATE st.study_relationships s
                        SET target_id = t.study_id
                        FROM nk.temp_study_ids t
                        WHERE s.target_sd_id = t.study_sd_id;";

                conn.Execute(sql_string);

                // Update status of records

                sql_string = @"UPDATE " + schema_name + @".study_relationships s
                        SET record_status_id = 5
                        WHERE s.record_status_id = 1;";

				conn.Execute(sql_string);
			}
		}


		public void LoadNewStudyContributors(string schema_name)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// action should depend on whether study id / source is the 'preferred ' for this study
				string sql_string = @"INSERT INTO st.study_contributors(study_id, 
                        title_type_id, title_text, title_lang_code, 
                        lang_usage_id, is_default, contains_html,
                        comparison_text, comments, date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.title_type_id, s.title_text, s.title_lang_code, 
                        s.lang_usage_id, s.is_default, s.contains_html,
                        s.comparison_text, s.comments, 
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_titles s
                        on t.study_sd_id = s.sd_id
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = true;";

				conn.Execute(sql_string);

				// with existing studies sources all titles should be non default
				// only titles not already in the system need to be added

				sql_string = @"INSERT INTO st.study_contributors(study_id, 
                        title_type_id, title_text, title_lang_code, 
                        lang_usage_id, is_default, contains_html,
                        comparison_text, comments, date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.title_type_id, s.title_text, s.title_lang_code, 
                        s.lang_usage_id, false, s.contains_html,
                        s.comparison_text, s.comments, 
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_titles s
                        on t.study_sd_id = s.sd_id
                        LEFT JOIN st.study_titles s2
                        ON s2.study_id = t.study_id
                        AND s.title_type_id = s2.title_type_id
                        AND s.title_text = s2.title_text
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = false 
                        AND s2.study_id is null;";

				conn.Execute(sql_string);

				// Update status of records

				sql_string = @"UPDATE " + schema_name + @".study_contributors s
                        SET record_status_id = 5
                        WHERE s.record_status_id = 1;";

				conn.Execute(sql_string);
			}
		}


		public void LoadNewStudyTopics(string schema_name)
		{
            using (var conn = new NpgsqlConnection(connString))
            {
                // action should depend on whether study id / source is the 'preferred ' for this study
                string sql_string = @"INSERT INTO st.study_topics(study_id, 
                        title_type_id, title_text, title_lang_code, 
                        lang_usage_id, is_default, contains_html,
                        comparison_text, comments, date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.title_type_id, s.title_text, s.title_lang_code, 
                        s.lang_usage_id, s.is_default, s.contains_html,
                        s.comparison_text, s.comments, 
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_titles s
                        on t.study_sd_id = s.sd_id
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = true;";

                conn.Execute(sql_string);

                // with existing studies sources all titles should be non default
                // only titles not already in the system need to be added

                sql_string = @"INSERT INTO st.study_topics(study_id, 
                        title_type_id, title_text, title_lang_code, 
                        lang_usage_id, is_default, contains_html,
                        comparison_text, comments, date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.title_type_id, s.title_text, s.title_lang_code, 
                        s.lang_usage_id, false, s.contains_html,
                        s.comparison_text, s.comments, 
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_titles s
                        on t.study_sd_id = s.sd_id
                        LEFT JOIN st.study_titles s2
                        ON s2.study_id = t.study_id
                        AND s.title_type_id = s2.title_type_id
                        AND s.title_text = s2.title_text
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = false 
                        AND s2.study_id is null;";

                conn.Execute(sql_string);

                // Update status of records

                sql_string = @"UPDATE " + schema_name + @".study_topics s
                        SET record_status_id = 5
                        WHERE s.record_status_id = 1;";

                conn.Execute(sql_string);
            }
        }


		public void LoadNewStudyFeatures(string schema_name)
		{
            using (var conn = new NpgsqlConnection(connString))
            {
                // action should depend on whether study id / source is the 'preferred ' for this study
                string sql_string = @"INSERT INTO st.study_features(study_id, 
                        title_type_id, title_text, title_lang_code, 
                        lang_usage_id, is_default, contains_html,
                        comparison_text, comments, date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.title_type_id, s.title_text, s.title_lang_code, 
                        s.lang_usage_id, s.is_default, s.contains_html,
                        s.comparison_text, s.comments, 
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_titles s
                        on t.study_sd_id = s.sd_id
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = true;";

                conn.Execute(sql_string);

                // with existing studies sources all titles should be non default
                // only titles not already in the system need to be added

                sql_string = @"INSERT INTO st.study_features(study_id, 
                        title_type_id, title_text, title_lang_code, 
                        lang_usage_id, is_default, contains_html,
                        comparison_text, comments, date_of_data, record_status_id)
                        SELECT t.study_id,
                        s.title_type_id, s.title_text, s.title_lang_code, 
                        s.lang_usage_id, false, s.contains_html,
                        s.comparison_text, s.comments, 
                        current_timestamp, s.record_status_id
						FROM nk.temp_study_ids t
                        INNER JOIN " + schema_name + @".study_titles s
                        on t.study_sd_id = s.sd_id
                        LEFT JOIN st.study_titles s2
                        ON s2.study_id = t.study_id
                        AND s.title_type_id = s2.title_type_id
                        AND s.title_text = s2.title_text
                        WHERE s.record_status_id = 1
                        AND t.is_preferred = false 
                        AND s2.study_id is null;";

                conn.Execute(sql_string);

                // Update status of records

                sql_string = @"UPDATE " + schema_name + @".study_features s
                        SET record_status_id = 5
                        WHERE s.record_status_id = 1;";

                conn.Execute(sql_string);
            }
        }


		public void DropTempStudyIdsTable()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_study_ids";
				conn.Execute(sql_string);
			}
		}

	}
}
