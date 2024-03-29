﻿using Dapper;
using Dapper.Contrib.Extensions;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataAggregator
{
    public class MonitorDataLayer : IMonitorDataLayer
    {

        private string connString;
        private Source source;
        NpgsqlConnectionStringBuilder builder;

        public MonitorDataLayer(LoggingHelper loggingHelper, ICredentials credentials)
        {
            builder = new NpgsqlConnectionStringBuilder();
            builder.Host = credentials.Host;
            builder.Username = credentials.Username;
            builder.Password = credentials.Password;

            builder.Database = "mon";
            connString = builder.ConnectionString;
        }

        public Source SourceParameters => source;


        public Source FetchSourceParameters(int source_id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                source = Conn.Get<Source>(source_id);
                return source;
            }
        }



        public void SetUpTempContextFTWs(ICredentials credentials, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string username = credentials.Username;
                string password = credentials.Password;

                string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                     schema sf;";
                conn.Execute(sql_string);

                sql_string = @"CREATE SERVER IF NOT EXISTS context
                               FOREIGN DATA WRAPPER postgres_fdw
                               OPTIONS (host 'localhost', dbname 'context');";
                conn.Execute(sql_string);

                sql_string = @"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                     SERVER context"
                     + @" OPTIONS (user '" + username + "', password '" + password + "');";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_lup cascade;
                     CREATE SCHEMA context_lup;
                     IMPORT FOREIGN SCHEMA lup
                     FROM SERVER context 
                     INTO context_lup;";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_ctx cascade;
                     CREATE SCHEMA context_ctx;
                     IMPORT FOREIGN SCHEMA ctx
                     FROM SERVER context 
                     INTO context_ctx;";
                conn.Execute(sql_string);
            }
        }


        public void DropTempContextFTWs(string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"DROP USER MAPPING IF EXISTS FOR CURRENT_USER
                     SERVER context;";
                conn.Execute(sql_string);

                sql_string = @"DROP SERVER IF EXISTS context CASCADE;";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS context_lup;";
                conn.Execute(sql_string);
                sql_string = @"DROP SCHEMA IF EXISTS context_ctx;";
                conn.Execute(sql_string);
            }
        }


        public string SetUpTempFTW(ICredentials credentials, string database_name, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string username = credentials.Username;
                string password = credentials.Password;     
                
                string sql_string = @"CREATE EXTENSION IF NOT EXISTS postgres_fdw
                                     schema core;";
                conn.Execute(sql_string);

                sql_string = @"CREATE SERVER IF NOT EXISTS " + database_name
                           + @" FOREIGN DATA WRAPPER postgres_fdw
                             OPTIONS (host 'localhost', dbname '" + database_name + "');";
                conn.Execute(sql_string);

                sql_string = @"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
                     SERVER " + database_name
                     + @" OPTIONS (user '" + username + "', password '" + password + "');";
                conn.Execute(sql_string);
                string schema_name = "";
                if (database_name == "mon")
                {
                    schema_name = database_name + "_sf";
                    sql_string = @"DROP SCHEMA IF EXISTS " + schema_name + @" cascade;
                     CREATE SCHEMA " + schema_name + @";
                     IMPORT FOREIGN SCHEMA sf
                     FROM SERVER " + database_name +
                         @" INTO " + schema_name + ";";
                }
                else
                {
                    schema_name = database_name + "_ad";
                    sql_string = @"DROP SCHEMA IF EXISTS " + schema_name + @" cascade;
                     CREATE SCHEMA " + schema_name + @";
                     IMPORT FOREIGN SCHEMA ad
                     FROM SERVER " + database_name +
                         @" INTO " + schema_name + ";";
                }
                conn.Execute(sql_string);
                return schema_name;
            }
         }


        public void DropTempFTW(string database_name, string connString)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string schema_name = "";
                if (database_name == "mon")
                {
                    schema_name = database_name + "_sf";
                }
                else
                {
                    schema_name = database_name + "_ad";
                }

                string sql_string = @"DROP USER MAPPING IF EXISTS FOR CURRENT_USER
                     SERVER " + database_name + ";";
                conn.Execute(sql_string);

                sql_string = @"DROP SERVER IF EXISTS " + database_name + " CASCADE;";
                conn.Execute(sql_string);

                sql_string = @"DROP SCHEMA IF EXISTS " + schema_name;
                conn.Execute(sql_string);
            }
        }



        public int GetNextAggEventId()
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = "select max(id) from sf.aggregation_events ";
                int? last_id = Conn.ExecuteScalar<int?>(sql_string);
                return (last_id == null) ? 100001 : (int)last_id + 1;
            }
        }

        public int GetLastAggEventId()
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                string sql_string = "select max(id) from sf.aggregation_events ";
                int? last_id = Conn.ExecuteScalar<int?>(sql_string);
                return (int)last_id;
            }
        }


        public int StoreAggregationEvent(AggregationEvent aggregation)
        {
            aggregation.time_ended = DateTime.Now;
            using (var conn = new NpgsqlConnection(connString))
            {
                return (int)conn.Insert<AggregationEvent>(aggregation);
            }

        }


        public IEnumerable<Source> RetrieveDataSources()
        {
            string sql_string = @"select id, preference_rating, database_name, 
                                  has_study_tables,	has_study_topics, has_study_features,
                                  has_study_contributors, has_study_countries, has_study_locations,
                                  has_study_references, has_study_relationships,
                                  has_object_datasets, has_object_dates, has_object_rights,
                                  has_object_relationships, has_object_pubmed_set 
                                from sf.source_parameters
                                where id > 100115 and id < 900000
                                order by preference_rating;";

            using (var conn = new NpgsqlConnection(connString))
            {
                return conn.Query<Source>(sql_string);
            }
        }

       

        public void DeleteSameEventDBStats(int agg_event_id)
        {
            string sql_string = "DELETE from sf.source_summaries ";
            sql_string += " where aggregation_event_id = " + agg_event_id.ToString();
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Execute(sql_string);
            }
        }


        public int GetRecNum(string table_name, string source_conn_string)
        {
            string sql_string = "SELECT count(*) from ad." + table_name;
            int? rec_num;
            using (var conn = new NpgsqlConnection(source_conn_string))
            {
                rec_num = conn.ExecuteScalar<int?>(sql_string);
            }
            return rec_num == null ? 0 : (int)rec_num;
        }


        public void DeleteSameEventSummaryStats(int agg_event_id)
        {
            string sql_string = "DELETE from sf.aggregation_summaries ";
            sql_string += " where aggregation_event_id = " + agg_event_id.ToString();
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Execute(sql_string);
            }
        }


        public int GetAggregateRecNum(string table_name, string schema_name, string source_conn_string)
        {
            string sql_string = "SELECT count(*) from " + schema_name + "." + table_name;
            int? rec_num;
            using (var conn = new NpgsqlConnection(source_conn_string))
            {
                rec_num = conn.ExecuteScalar<int?>(sql_string);
            }
            return rec_num == null ? 0 : (int)rec_num;
        }


        public void StoreSourceSummary(SourceSummary sm)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Insert<SourceSummary>(sm);
            }
        }


        public void StoreAggregationSummary(AggregationSummary asm)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Insert<AggregationSummary>(asm);
            }
        }


        public void DeleteSameEventObjectStats(int agg_event_id)
        {
            string sql_string = "DELETE from sf.aggregation_object_numbers ";
            sql_string += " where aggregation_event_id = " + agg_event_id.ToString();
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Execute(sql_string);
            }
        }


        public List<AggregationObjectNum> GetObjectTypes(int aggregation_event_id, string dest_conn_string)
        {
            string sql_string = @"SELECT "
                    + aggregation_event_id.ToString() + @" as aggregation_event_id, 
                    d.object_type_id, 
                    t.name as object_type_name,
                    count(d.id) as number_of_type
                    from ob.data_objects d
                    inner join context_lup.object_types t
                    on d.object_type_id = t.id
                    group by object_type_id, t.name
                    order by count(d.id) desc";

            using (var conn = new NpgsqlConnection(dest_conn_string))
            {
                return conn.Query<AggregationObjectNum>(sql_string).ToList();
            }

        }


        public void RecreateStudyStudyLinksTable()
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = "DROP TABLE IF EXISTS sf.study_study_link_data ";
                conn.Execute(sql_string);

                sql_string = @"CREATE TABLE sf.study_study_link_data
                (
                   id                   int NOT NULL GENERATED BY DEFAULT AS IDENTITY(INCREMENT 1 START 10000001)
                 , source_id            int
                 , source_name          varchar
                 , other_source_id      int
                 , other_source_name    varchar
                 , number_in_other_source int
                )";
                conn.Execute(sql_string);
            }
        }

        public List<StudyStudyLinkData> GetStudyStudyLinkData(int aggregation_event_id, string dest_conn_string)
        {
            string sql_string = @"SELECT 
                    k.source_id, 
                    d1.repo_name as source_name,
                    k.preferred_source_id as other_source_id,
                    d2.repo_name as other_source_name,
                    count(preferred_sd_sid) as number_in_other_source
                    from nk.study_study_links k
                    inner join mon_sf.source_parameters d1
                    on k.source_id = d1.id
                    inner join mon_sf.source_parameters d2
                    on k.preferred_source_id = d2.id
                    group by source_id, preferred_source_id, d1.repo_name, d2.repo_name;";

            using (var conn = new NpgsqlConnection(dest_conn_string))
            {
                return conn.Query<StudyStudyLinkData>(sql_string).ToList();
            }
        }

        public List<StudyStudyLinkData> GetStudyStudyLinkData2(int aggregation_event_id, string dest_conn_string)
        {
            string sql_string = @"SELECT 
                    k.preferred_source_id as source_id, 
                    d2.repo_name as source_name,
                    k.source_id as other_source_id,
                    d1.repo_name as other_source_name,
                    count(sd_sid) as number_in_other_source
                    from nk.study_study_links k
                    inner join mon_sf.source_parameters d1
                    on k.source_id = d1.id
                    inner join mon_sf.source_parameters d2
                    on k.preferred_source_id = d2.id
                    group by preferred_source_id, source_id, d2.repo_name, d1.repo_name;;";

            using (var conn = new NpgsqlConnection(dest_conn_string))
            {
                return conn.Query<StudyStudyLinkData>(sql_string).ToList();
            }
        }



        public ulong StoreObjectNumbers(PostgreSQLCopyHelper<AggregationObjectNum> copyHelper, 
                                         IEnumerable<AggregationObjectNum> entities)
        {
            // stores the study id data in a temporary table
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }


        public ulong StoreStudyLinkNumbers(PostgreSQLCopyHelper<StudyStudyLinkData> copyHelper,
                                        IEnumerable<StudyStudyLinkData> entities)
        {
            // stores the study id data in a temporary table
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                return copyHelper.SaveAll(conn, entities);
            }
        }

    }

}

