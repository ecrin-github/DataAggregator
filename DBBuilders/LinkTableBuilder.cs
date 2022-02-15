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

        /*
            // One off builds

            CREATE TABLE nk.study_identifiers(
               id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 2000001 INCREMENT BY 1) PRIMARY KEY
             , study_id                 INT             NULL
             , source_id                INT             NULL
             , sd_sid                   VARCHAR         NULL
             , is_preferred             BOOLEAN         NULL
             , datetime_of_data_fetch   TIMESTAMPTZ     NULL
             , date_id_added            TIMESTAMPTZ     NULL DEFAULT Now()
             , match_status             INT             NULL
            );
            CREATE INDEX study_ids_studyid ON nk.study_identifiers(study_id);
            CREATE INDEX study_ids_sdsidsource ON nk.study_identifiers(source_id, sd_sid);
      
		   
            CREATE TABLE nk.data_object_identifiers(
               id                       INT             NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 10000001 INCREMENT BY 1) PRIMARY KEY
             , object_id                INT             NULL
             , source_id                INT             NOT NULL
             , sd_oid                   VARCHAR         NULL
             , object_type_id			INT             NULL
             , title                    VARCHAR         NULL
             , is_preferred_object      BOOLEAN         NULL
             , parent_study_source_id   INT             NULL
             , parent_study_sd_sid      VARCHAR         NULL
             , parent_study_id          INT             NULL
             , is_preferred_study       BOOLEAN         NULL
             , datetime_of_data_fetch   TIMESTAMPTZ     NULL
             , date_id_added            TIMESTAMPTZ     NULL DEFAULT Now()
             , match_status             INT             NULL	
            );
            CREATE INDEX object_ids_objectid ON nk.data_object_identifiers(object_id);
            CREATE INDEX object_ids_sdidsource ON nk.data_object_identifiers(source_id, sd_oid);
          */

        public void create_table_linked_study_groups()
        {
            string sql_string = @"CREATE TABLE nk.linked_study_groups(
                source_id                INT             NULL
              , sd_sid                   VARCHAR         NULL
              , relationship_id          INT             NULL
              , target_sd_sid            VARCHAR         NULL
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
        CREATE INDEX study_object_links_objectid ON nk.study_object_links(object_id);
        CREATE INDEX study_object_links_studyid ON nk.study_object_links(study_id);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_table_study_study_links()
        {
            string sql_string = @"CREATE TABLE nk.study_study_links(
                source_id                INT             NULL
              , sd_sid                   VARCHAR         NULL
              , preferred_sd_sid         VARCHAR         NULL
              , preferred_source_id      INT             NULL
              );";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }

    }
}
