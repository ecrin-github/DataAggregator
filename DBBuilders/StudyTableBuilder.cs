using Dapper;
using Npgsql;


namespace DataAggregator
{
	public class StudyTableBuilder
	{
		string db_conn;

		public StudyTableBuilder(string _db_conn)
		{
			db_conn = _db_conn;
		}

		public void drop_table(string table_name)
		{
			string sql_string = @"DROP TABLE IF EXISTS st." + table_name;
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_studies()
		{
			string sql_string = @"CREATE TABLE st.studies(
                id                     INT             NOT NULL
			  , display_title          VARCHAR         NULL
              , title_lang_code        VARCHAR         NULL
			  , brief_description      VARCHAR         NULL
              , bd_contains_html       BOOLEAN         NULL
			  , data_sharing_statement VARCHAR         NULL
              , dss_contains_html      BOOLEAN         NULL
			  , study_start_year       INT             NULL
			  , study_start_month      INT             NULL
			  , study_type_id          INT             NULL
			  , study_status_id        INT             NULL
			  , study_enrolment        INT             NULL
			  , study_gender_elig_id   INT             NULL
			  , min_age                INT             NULL
			  , min_age_units_id       INT             NULL
			  , max_age                INT             NULL
			  , max_age_units_id       INT             NULL
			  , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
			);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_identifiers()
		{
			string sql_string = @"CREATE TABLE st.study_identifiers(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , study_id               INT             NOT NULL
			  , identifier_type_id     INT             NULL
			  , identifier_value       VARCHAR         NULL
			  , identifier_org_id      INT             NULL
			  , identifier_org         VARCHAR         NULL
			  , identifier_date        VARCHAR         NULL
              , identifier_link        VARCHAR         NULL
			  , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
			);
            CREATE INDEX study_identifiers_study_id ON st.study_identifiers(study_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_relationships()
		{
			string sql_string = @"CREATE TABLE st.study_relationships(
                id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 20000001 INCREMENT BY 1) PRIMARY KEY
			  , study_id               INT             NOT NULL
			  , relationship_type_id   INT             NULL
			  , target_study_id        VARCHAR         NULL
			  , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
			);
            CREATE INDEX study_relationships_study_id ON st.study_relationships(study_id);
			CREATE INDEX study_relationships_target_study_id ON st.study_relationships(target_study_id);"; 

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_titles()
		{
			string sql_string = @"CREATE TABLE st.study_titles(
                id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 20000001 INCREMENT BY 1) PRIMARY KEY
			  , study_id               INT             NOT NULL
			  , title_type_id          INT             NULL
			  , title_text             VARCHAR         NULL
			  , lang_code              VARCHAR         NULL
			  , lang_usage_id          INT             NULL
			  , is_default             BOOLEAN         NULL
			  , comments               VARCHAR         NULL
			  , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
			);
            CREATE INDEX study_titles_study_id ON st.study_titles(study_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_contributors()
		{
			string sql_string = @"CREATE TABLE st.study_contributors(
                id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 20000001 INCREMENT BY 1) PRIMARY KEY
			  , study_id               INT             NOT NULL
     		  , contrib_type_id        INT             NULL
			  , is_individual          BOOLEAN         NULL
			  , organisation_id        INT             NULL
              , organisation_name      VARCHAR         NULL
			  , person_id              INT             NULL
			  , person_given_name      VARCHAR         NULL
			  , person_family_name     VARCHAR         NULL
			  , person_full_name       VARCHAR         NULL
			  , person_identifier      VARCHAR         NULL
			  , identifier_type        VARCHAR         NULL
			  , person_affiliation     VARCHAR         NULL
			  , affil_org_id           VARCHAR         NULL
			  , affil_org_id_type      VARCHAR         NULL
			  , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
			);
            CREATE INDEX study_contributors_study_id ON st.study_contributors(study_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_topics()
		{
			string sql_string = @"CREATE TABLE st.study_topics(
                id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 20000001 INCREMENT BY 1) PRIMARY KEY
			  , study_id               INT             NOT NULL
			  , topic_type_id          INT             NULL
              , mesh_coded             BOOLEAN         NULL
              , topic_code             VARCHAR         NULL
			  , topic_value            VARCHAR         NULL
			  , topic_qualcode         VARCHAR         NULL
			  , topic_qualvalue        VARCHAR         NULL
			  , original_ct_id         INT             NULL
              , original_ct_code       VARCHAR         NULL
			  , original_value         VARCHAR         NULL
			  , comments               VARCHAR         NULL
			  , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
			);
            CREATE INDEX study_topics_study_id ON st.study_topics(study_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_features()
		{
			string sql_string = @"CREATE TABLE st.study_features(
                id                     INT             GENERATED ALWAYS AS IDENTITY (START WITH 20000001 INCREMENT BY 1) PRIMARY KEY
			  , study_id               INT             NOT NULL
			  , feature_type_id        INT             NULL
			  , feature_value_id       INT             NULL
			  , aggregated_on          TIMESTAMPTZ     NOT NULL DEFAULT Now()
			);
            CREATE INDEX study_features_study_id ON st.study_features(study_id);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

	}
}
