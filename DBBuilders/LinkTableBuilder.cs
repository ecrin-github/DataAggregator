using Dapper;
using Npgsql;


namespace DataAggregator
{
	public class LinkTableBuilder
	{
		string db_conn;

		public LinkTableBuilder(string _db_conn)
		{
			db_conn = _db_conn;
		}

		public void drop_table(string table_name)
		{
			string sql_string = @"DROP TABLE IF EXISTS nk." + table_name;
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


        public void create_table_all_ids_data_objects()
        {
            string sql_string = @"CREATE TABLE nk.all_ids_data_objects(
                id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10000001 INCREMENT BY 1) PRIMARY KEY
              , object_id                INT             NULL
              , object_ad_id             INT             NULL
              , object_source_id         INT             NOT NULL
              , object_sd_id             VARCHAR         NULL
              , datetime_of_data_fetch   TIMESTAMPTZ     NULL
              , parent_study_id          INT             NULL
              , is_preferred             BOOLEAN         NULL
              , is_study_preferred       BOOLEAN         NULL
              , datetime_of_link         TIMESTAMPTZ     NULL
             );
            CREATE INDEX object_all_ids_adidsource ON nk.all_ids_data_objects USING btree(object_source_id, object_ad_id);
            CREATE INDEX object_all_ids_objectid ON nk.all_ids_data_objects USING btree(object_id);
            CREATE INDEX object_all_ids_sdidsource ON nk.all_ids_data_objects USING btree(object_source_id, object_sd_id);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_table_all_ids_studies()
        {

            string sql_string = @"CREATE TABLE nk.all_ids_studies(
                id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 3000001 INCREMENT BY 1) PRIMARY KEY
              , study_id                 INT             NULL
              , study_ad_id              INT             NULL
              , study_source_id          INT             NULL
              , study_sd_id              VARCHAR         NULL
              , datetime_of_data_fetch   TIMESTAMPTZ     NULL
              , is_preferred             BOOLEAN         NULL
              , datetime_of_link         TIMESTAMPTZ     NULL
             );
            CREATE INDEX study_all_ids_adidsource ON nk.all_ids_studies USING btree(study_source_id, study_ad_id);
            CREATE INDEX study_all_ids_sdidsource ON nk.all_ids_studies USING btree(study_source_id, study_sd_id);
            CREATE INDEX study_all_ids_studyid ON nk.all_ids_studies USING btree(study_id);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_table_all_links()
        {
            string sql_string = @"CREATE TABLE nk.all_links(
                id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10000001 INCREMENT BY 1) PRIMARY KEY
              , study_id                 INT             NULL
              , study_source_id          INT             NOT NULL
              , study_ad_id              INT             NOT NULL
              , study_sd_id              VARCHAR         NOT NULL
              , use_link                 INT             NOT NULL DEFAULT 1
              , object_sd_id             VARCHAR         NOT NULL
              , object_ad_id             INT             NOT NULL
              , object_source_id         INT             NOT NULLger NOT NULL
              , object_id                INT             NULL
              );
           CREATE INDEX all_links_objectadid ON nk.all_links USING btree(object_ad_id);
           CREATE INDEX all_links_objectid ON nk.all_links USING btree(object_id);
           CREATE INDEX all_links_studyadid ON nk.all_links USING btree(study_ad_id);
           CREATE INDEX all_links_studyid ON nk.all_links USING btree(study_id);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_table_linked_study_groups()
        {
            string sql_string = @"CREATE TABLE nk.linked_study_groups(
                source_id                INT             NULL
              , source_sd_id             VARCHAR         NULL
              , relationship_id          INT             NULL
              , target_sd_id             VARCHAR         NULL
              , target_source_id         INT             NULL
            );";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_table_study_object_links()
        {
            string sql_string = @"CREATE TABLE nk.study_object_links(
                id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10000001 INCREMENT BY 1) PRIMARY KEY
              , study_id                 INT             NOT NULL
              , object_id                INT             NOT NULL
        );
        CREATE INDEX study_object_links_objectid ON nk.study_object_links USING btree(object_id);
        CREATE INDEX study_object_links_studyid ON nk.study_object_links USING btree(study_id);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_table_study_study_links()
        {
            string sql_string = @"CREATE TABLE nk.study_study_links(
                source_id                INT             NULL
              , sd_id                    VARCHAR         NULL
              , preferred_sd_id          VARCHAR         NULL
              , preferred_source_id      INT             NULL
              );";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

        public void create_table_temp_study_ids()
        {
            string sql_string = @"CREATE TABLE nk.temp_study_ids(
                study_id                 INT             NULL
              , source_id                INT             NULL
              , sd_id                    VARCHAR         NULL
              , datetime_of_data_fetch   TIMESTAMPTZ     NULL
              , is_preferred             BOOLEAN         NULL
              , status                   INT             NULL
              , date_of_study_data       TIMESTAMPTZ     NULL
             );";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }
    }
}
