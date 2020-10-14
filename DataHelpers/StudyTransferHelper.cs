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
		string connString;
        DBUtilities db;

        public StudyDataTransferrer(DataLayer _repo)
		{
			repo = _repo;
			connString = repo.ConnString;
            db = new DBUtilities(connString);
		}


		public void SetUpTempStudyIdsTable()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_ids;
                        CREATE TABLE nk.temp_study_ids(
				        study_id                 INT
                      , source_id                INT
                      , sd_sid                   VARCHAR
                      , is_preferred             BOOLEAN
                      , datetime_of_data_fetch   TIMESTAMPTZ
                      ); ";
				conn.Execute(sql_string);
			}
		}

        public IEnumerable<StudyIds> FetchStudyIds(int source_id, string source_conn_string)
        {
            using (var conn = new NpgsqlConnection(source_conn_string))
            {
                string sql_string = @"select " + source_id.ToString() + @" as source_id, 
                          sd_sid, datetime_of_data_fetch
                          from ad.studies";

                return conn.Query<StudyIds>(sql_string);
            }
        }


        public ulong StoreStudyIds(PostgreSQLCopyHelper<StudyIds> copyHelper, IEnumerable<StudyIds> entities)
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
                // Does any study id correspond to a study already all_ids_studies 
                // table, that is linked to it via study-study link table.
                // Such a study will match the left hand side of the study-study 
                // link table (the one to be replacwed), and take on the study_id 
                // used for the 'preferred' right hand side. This should already exist
                // because addition of studies is donme in the order 'more preferred first'.

                string sql_string = @"UPDATE nk.temp_study_ids t
		                   SET study_id = s.study_id, is_preferred = false
                           FROM nk.study_study_links k
                                INNER JOIN nk.all_ids_studies s
                                ON k.preferred_sd_sid = s.sd_sid
                                AND k.preferred_source_id = s.source_id
                           WHERE t.sd_sid = k.sd_sid
                           AND t.source_id =  k.source_id;";
				conn.Execute(sql_string);
			}
		}


		public void UpdateAllStudyIdsTable(int source_id)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// Add the new study id records to the all Ids table

				string sql_string = @"INSERT INTO nk.all_ids_studies
                            (study_id, source_id, sd_sid, 
                             datetime_of_data_fetch, is_preferred)
                             select study_id, source_id, sd_sid, 
                             datetime_of_data_fetch, is_preferred
                             from nk.temp_study_ids";

				conn.Execute(sql_string);

				// Where the study_ids are null they can take on the value of the 
				// record id.

				sql_string = @"UPDATE nk.all_ids_studies
                            SET study_id = id, is_preferred = true
                            WHERE study_id is null
                            AND source_id = " + source_id.ToString();

				conn.Execute(sql_string);

				// 'Back-update' the study temp table using the newly created study_ids
				// now all should be done...

				sql_string = @"UPDATE nk.temp_study_ids t
		                   SET study_id = a.study_id, is_preferred = true
                           FROM nk.all_ids_studies a
                           WHERE t.source_id = a.source_id
                           AND t.sd_sid = a.sd_sid
                           AND t.study_id is null;";

				conn.Execute(sql_string);
			}
		}


		
		public void LoadStudies(string schema_name)
		{
			
            // Insert the study data unless it is already in under another 
            // id (i.e. t.is_preferred = false).

			string sql_string = @"INSERT INTO st.studies(id, 
                    display_title, title_lang_code, brief_description, bd_contains_html,
                    data_sharing_statement, dss_contains_html,
                    study_start_year, study_start_month, study_type_id, study_status_id,
                    study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                    max_age, max_age_units_id)
                    SELECT t.study_id,
                    s.display_title, s.title_lang_code, s.brief_description, s.bd_contains_html,
                    s.data_sharing_statement, s.dss_contains_html,
                    s.study_start_year, s.study_start_month, s.study_type_id, s.study_status_id,
                    s.study_enrolment, s.study_gender_elig_id, s.min_age, s.min_age_units_id,
                    s.max_age, s.max_age_units_id
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".studies s
                    on t.sd_sid = s.sd_sid
                    WHERE t.is_preferred = true ";

            db.ExecuteTransferSQL(sql_string, schema_name, "studies", "preferred");

            // Note that the statement below also updates studies that are not added as new
            // (because they equate to existing studies) but which were new in the 
            // source data.

            db.Update_SourceTable_ExportDate(schema_name, "studies");
		}


		public void LoadStudyIdentifiers(string schema_name)
		{
            // For 'preferred' study Ids add all identifiers.

            string sql_string = @"INSERT INTO st.study_identifiers(study_id, 
                    identifier_type_id, identifier_org_id, identifier_org, 
                    identifier_value, identifier_date, identifier_link)
                    SELECT t.study_id,
                    s.identifier_type_id, s.identifier_org_id, s.identifier_org, 
                    s.identifier_value, s.identifier_date, s.identifier_link
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_identifiers s
                    on t.sd_sid = s.sd_sid
                    WHERE t.is_preferred = true ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_identifiers", "preferred");

            // For 'non-preferred' study Ids add only new identifiers.

            sql_string = @"INSERT INTO st.study_identifiers(study_id, 
                    identifier_type_id, identifier_org_id, identifier_org, 
                    identifier_value, identifier_date, identifier_link)
                    SELECT t.study_id,
                    s.identifier_type_id, s.identifier_org_id, s.identifier_org, 
                    s.identifier_value, s.identifier_date, s.identifier_link
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_identifiers s
                    on t.sd_sid = s.sd_sid
                    LEFT JOIN st.study_identifiers s2
                    ON s2.study_id = t.study_id
                    AND s.identifier_type_id = s2.identifier_type_id
                    AND s.identifier_value = s2.identifier_value
                    WHERE t.is_preferred = false 
                    AND s2.study_id is null ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_identifiers", "non-preferred");

            db.Update_SourceTable_ExportDate(schema_name, "study_identifiers");
		}


		public void LoadStudyTitles(string schema_name)
		{
            // For 'preferred' study Ids add all titles.

            string sql_string = @"INSERT INTO st.study_titles(study_id, 
                    title_type_id, title_text, lang_code, 
                    lang_usage_id, is_default, comments)
                    SELECT t.study_id,
                    s.title_type_id, s.title_text, s.lang_code, 
                    s.lang_usage_id, s.is_default, s.comments
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_titles s
                    on t.sd_sid = s.sd_sid
                    WHERE t.is_preferred = true ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_titles", "preferred");

            // For 'non-preferred' study Ids add only new titles.

            sql_string = @"INSERT INTO st.study_titles(study_id, 
                    title_type_id, title_text, lang_code, 
                    lang_usage_id, is_default, comments)
                    SELECT t.study_id,
                    s.title_type_id, s.title_text, s.lang_code, 
                    s.lang_usage_id, false, s.comments
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_titles s
                    on t.sd_sid = s.sd_sid
                    LEFT JOIN st.study_titles s2
                    ON s2.study_id = t.study_id
                    AND s.title_type_id = s2.title_type_id
                    AND s.title_text = s2.title_text
                    WHERE t.is_preferred = false 
                    AND s2.study_id is null ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_titles", "non-preferred");

            // Update status of records

            db.Update_SourceTable_ExportDate(schema_name, "study_titles");
		}


		public void LoadStudyContributors(string schema_name)
		{
			// For 'preferred' study Ids add all contributors.

			string sql_string = @"INSERT INTO st.study_contributors(study_id, 
                    contrib_type_id, is_individual, organisation_id, 
                    organisation_name, person_id, person_given_name,
                    person_family_name, person_full_name, person_identifier,
                    identifier_type, affil_org_id, affil_org_id_type)
                    SELECT t.study_id,
                    s.contrib_type_id, s.is_individual, s.organisation_id, 
                    s.organisation_name, s.person_id, s.person_given_name,
                    s.person_family_name, s.person_full_name, s.person_identifier,
                    s.identifier_type, s.affil_org_id, s.affil_org_id_type
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_contributors s
                    on t.sd_sid = s.sd_sid
                    WHERE t.is_preferred = true ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_contributors", "preferred");

            // For 'non-preferred' study Ids add only new contributors.

            sql_string = @"INSERT INTO st.study_contributors(study_id, 
                    contrib_type_id, is_individual, organisation_id, 
                    organisation_name, person_id, person_given_name,
                    person_family_name, person_full_name, person_identifier,
                    identifier_type, affil_org_id, affil_org_id_type)
                    SELECT t.study_id,
                    s.contrib_type_id, s.is_individual, s.organisation_id, 
                    s.organisation_name, s.person_id, s.person_given_name,
                    s.person_family_name, s.person_full_name, s.person_identifier,
                    s.identifier_type, s.affil_org_id, s.affil_org_id_type
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_contributors s
                    on t.sd_sid = s.sd_sid
                    LEFT JOIN st.study_contributors s2
                    ON s2.study_id = t.study_id
                    AND s.contrib_type_id = s2.contrib_type_id
                    AND (s.organisation_name = s2.organisation_name or s.person_full_name = s2.person_full_name)
                    WHERE t.is_preferred = false 
                    AND s2.study_id is null ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_contributors", "non-preferred");

            db.Update_SourceTable_ExportDate(schema_name, "study_contributors");
		}


		public void LoadStudyTopics(string schema_name)
		{
            // For 'preferred' study Ids add all topics.

            string sql_string = @"INSERT INTO st.study_topics(study_id, 
                    topic_type_id, mesh_coded, topic_code, 
                    topic_value, topic_qualcode, topic_qualvalue,
                    original_ct_id, original_ct_code, original_value, comments)
                    SELECT t.study_id,
                    s.topic_type_id, s.mesh_coded, s.topic_code, 
                    s.topic_value, s.topic_qualcode, s.topic_qualvalue,
                    s.original_ct_id, s.original_ct_code, s.original_value, s.comments
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_topics s
                    on t.sd_sid = s.sd_sid
                    WHERE t.is_preferred = true ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_topics", "preferred");

            // For 'non-preferred' study Ids add only new topics.

            sql_string = @"INSERT INTO st.study_topics(study_id, 
                    topic_type_id, mesh_coded, topic_code, 
                    topic_value, topic_qualcode, topic_qualvalue,
                    original_ct_id, original_ct_code, original_value, comments)
                    SELECT t.study_id,
                    s.topic_type_id, s.mesh_coded, s.topic_code, 
                    s.topic_value, s.topic_qualcode, s.topic_qualvalue,
                    s.original_ct_id, s.original_ct_code, s.original_value, s.comments
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_topics s
                    on t.sd_sid = s.sd_sid
                    LEFT JOIN st.study_topics s2
                    ON s2.study_id = t.study_id
                    AND s.topic_value = s2.topic_value
                    WHERE t.is_preferred = false 
                    AND s2.study_id is null ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_topics", "non-preferred");

            db.Update_SourceTable_ExportDate(schema_name, "study_topics");
        }


		public void LoadStudyFeatures(string schema_name)
		{
            // For 'preferred' study Ids add all features.

            string sql_string = @"INSERT INTO st.study_features(study_id, 
                    feature_type_id, feature_value_id)
                    SELECT t.study_id,
                    s.feature_type_id, s.feature_value_id
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_features s
                    on t.sd_sid = s.sd_sid
                    WHERE t.is_preferred = true ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_features", "preferred");

            // For 'non-preferred' study Ids add only new feature types.

            sql_string = @"INSERT INTO st.study_features(study_id, 
                    feature_type_id, feature_value_id)
                    SELECT t.study_id,
                    s.feature_type_id, s.feature_value_id
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_features s
                    on t.sd_sid = s.sd_sid
                    LEFT JOIN st.study_features s2
                    ON s2.study_id = t.study_id
                    AND s.feature_type_id = s2.feature_type_id
                    WHERE t.is_preferred = false 
                    AND s2.study_id is null ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_features", "non-preferred");

            // Update status of records

            db.Update_SourceTable_ExportDate(schema_name, "study_features");
        }


        public void LoadStudyRelationShips(string schema_name)
        {
            // For 'preferred' study Ids add all relationships.

            string sql_string = @"INSERT INTO st.study_relationships(study_id, 
                    relationship_id, comments)
                    SELECT t.study_id,
                    s.relationship_id, s.comments
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_relationships s
                    on t.sd_sid = s.sd_sid
                    WHERE t.is_preferred = true ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_relationships", "preferred");

            // For 'non-preferred' study Ids add only new relationships types.

            sql_string = @"INSERT INTO st.study_relationships(study_id, 
                    relationship_id, comments)
                    SELECT t.study_id,
                    s.relationship_id, s.comments
					FROM nk.temp_study_ids t
                    INNER JOIN " + schema_name + @".study_relationships s
                    on t.sd_sid = s.sd_sid
                    LEFT JOIN st.study_relationships s2
                    ON s2.study_id = t.study_id
                    AND s.relationship_id = s2.relationship_id
                    WHERE t.is_preferred = false 
                    AND s2.study_id is null ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_relationships", "non-preferred");

            // insert target study id, using sd_sid to find it in the temp studies table
            // N.B. These relationships are defined within the same source...
            // (Cross source relationships are defined in the links (nk) schema)

            sql_string = @"UPDATE st.study_relationships r
                    SET target_study_id = tt.target_study_id
                    FROM 
                        (SELECT t.study_id, t2.study_id as target_study_id
                            FROM nk.temp_study_ids t 
                            INNER JOIN " + schema_name + @".study_relationships s 
                            on t.sd_sid = s.sd_sid
                            INNER JOIN nk.temp_study_ids t2
                            on s.target_sd_sid = t2.sd_sid) tt
                    WHERE r.study_id = tt.study_id ";

            db.ExecuteTransferSQL(sql_string, schema_name, "study_relationships", "updating target ids");

            db.Update_SourceTable_ExportDate(schema_name, "study_relationships");
        }


        public void DropTempStudyIdsTable()
		{
			string sql_string = "DROP TABLE IF EXISTS nk.temp_study_ids";
			db.ExecuteSQL(sql_string);
		}

	}
}
