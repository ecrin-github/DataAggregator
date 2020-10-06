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
			string sql_string = @"DROP TABLE IF EXISTS sd." + table_name;
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_studies()
		{
			string sql_string = @"CREATE TABLE sd.studies(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 VARCHAR         NOT NULL
			  , display_title          VARCHAR         NULL
              , title_lang_code        VARCHAR         NULL default 'en'
			  , brief_description      VARCHAR         NULL
              , bd_contains_html       BOOLEAN         NULL	default false
			  , data_sharing_statement VARCHAR         NULL
              , dss_contains_html      BOOLEAN         NULL	default false
			  , study_start_year       INT             NULL
			  , study_start_month      INT             NULL
			  , study_type_id          INT             NULL
			  , study_type             VARCHAR         NULL
			  , study_status_id        INT             NULL
			  , study_status           VARCHAR         NULL
			  , study_enrolment        INT             NULL
			  , study_gender_elig_id   INT             NULL
			  , study_gender_elig      VARCHAR         NULL
			  , min_age                INT             NULL
			  , min_age_units_id       INT             NULL
			  , min_age_units          VARCHAR         NULL
			  , max_age                INT             NULL
			  , max_age_units_id       INT             NULL
			  , max_age_units          VARCHAR         NULL
			  , datetime_of_data_fetch TIMESTAMPTZ     NULL
              , record_hash            CHAR(32)        NULL
              , study_full_hash        CHAR(32)        NULL
			);
            CREATE INDEX studies_sid ON sd.studies(sd_sid);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_identifiers()
		{
			string sql_string = @"CREATE TABLE sd.study_identifiers(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 VARCHAR         NOT NULL
			  , identifier_value       VARCHAR         NULL
			  , identifier_type_id     INT             NULL
			  , identifier_type        VARCHAR         NULL
			  , identifier_org_id      INT             NULL
			  , identifier_org         VARCHAR         NULL
			  , identifier_date        VARCHAR         NULL
			  , identifier_link        VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_identifiers_sd_sid ON sd.study_identifiers(sd_sid);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_relationships()
		{
			string sql_string = @"CREATE TABLE sd.study_relationships(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 VARCHAR         NOT NULL
			  , relationship_type_id   INT             NULL
			  , relationship_type      VARCHAR         NULL
			  , target_sd_sid          VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_relationships_sd_sid ON sd.study_relationships(sd_sid);
			CREATE INDEX study_relationships_target_sd_sid ON sd.study_relationships(target_sd_sid);"; 

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_references()
		{
			string sql_string = @"CREATE TABLE sd.study_references(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 VARCHAR         NOT NULL
			  , pmid                   VARCHAR         NULL
			  , citation               VARCHAR         NULL
			  , doi                    VARCHAR         NULL	
			  , comments               VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_references_sd_sid ON sd.study_references(sd_sid);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_titles()
		{
			string sql_string = @"CREATE TABLE sd.study_titles(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 VARCHAR         NOT NULL
			  , title_type_id          INT             NULL
			  , title_type             VARCHAR         NULL
			  , title_text             VARCHAR         NULL
			  , lang_code              VARCHAR         NOT NULL default 'en'
			  , lang_usage_id          INT             NOT NULL default 11
			  , is_default             BOOLEAN         NULL
			  , comments               VARCHAR         NULL
			  , comparison_text        VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_titles_sd_sid ON sd.study_titles(sd_sid);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_contributors()
		{
			string sql_string = @"CREATE TABLE sd.study_contributors(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 VARCHAR         NOT NULL
     		  , contrib_type_id        INT             NULL
			  , contrib_type           VARCHAR         NULL
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
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_contributors_sd_sid ON sd.study_contributors(sd_sid);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_topics()
		{
			string sql_string = @"CREATE TABLE sd.study_topics(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 CHAR(24)        NOT NULL
			  , topic_type_id          INT             NULL
			  , topic_type             VARCHAR         NULL
              , mesh_coded             BOOLEAN         NULL
              , topic_code             VARCHAR         NULL
			  , topic_value            VARCHAR         NULL
			  , topic_qualcode         VARCHAR         NULL
			  , topic_qualvalue        VARCHAR         NULL
			  , original_ct_id         INT             NULL
              , original_ct_code       VARCHAR         NULL
			  , original_value         VARCHAR         NULL
			  , comments               VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_topics_sd_sid ON sd.study_topics(sd_sid);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_features()
		{
			string sql_string = @"CREATE TABLE sd.study_features(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 VARCHAR         NOT NULL
			  , feature_type_id        INT             NULL
			  , feature_type           VARCHAR         NULL
			  , feature_value_id       INT             NULL
			  , feature_value          VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_features_sid ON sd.study_features(sd_sid);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_study_links()
		{
			string sql_string = @"CREATE TABLE sd.study_links(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 VARCHAR         NOT NULL
			  , link_label             VARCHAR         NULL
			  , link_url               VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_links_sd_sid ON sd.study_links(sd_sid);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_ipd_available()
		{
			string sql_string = @"CREATE TABLE sd.study_ipd_available(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 VARCHAR         NOT NULL
			  , ipd_id                 VARCHAR         NULL
			  , ipd_type               VARCHAR         NULL
		      , ipd_url                VARCHAR         NULL
			  , ipd_comment            VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
			);
            CREATE INDEX study_ipd_available_sd_sid ON sd.study_ipd_available(sd_sid);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_hashes()
		{
			string sql_string = @"CREATE TABLE sd.study_hashes(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
			  , sd_sid                 VARCHAR         NOT NULL
			  , hash_type_id           INT             NULL
			  , hash_type              VARCHAR         NULL
			  , composite_hash         CHAR(32)        NULL
			);
            CREATE INDEX study_hashes_sd_sid ON sd.study_hashes(sd_sid);";

			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

	}
}
