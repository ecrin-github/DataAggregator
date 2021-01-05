using Dapper;
using Dapper.Contrib.Extensions;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataAggregator
{
    public class LoggingDataLayer
    {
        private string connString;
        private string mdr_connString;
        private Source source;
        private string sql_file_select_string;
        private string host;
        private string user;
        private string password;

        private string logfile_startofpath;
        private string logfile_path;
        private StreamWriter sw;

        /// <summary>
        /// Parameterless constructor is used to automatically build
        /// the connection string, using an appsettings.json file that 
        /// has the relevant credentials (but which is not stored in GitHub).
        /// </summary>
        /// 
        public LoggingDataLayer()
        {
            IConfigurationRoot settings = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();

            host = settings["host"];
            user = settings["user"];
            password = settings["password"];

            builder.Host = host;
            builder.Username = user;
            builder.Password = password;
            builder.Database = "mon";
            connString = builder.ConnectionString;

            builder.Database = "mdr";
            mdr_connString = builder.ConnectionString;

            logfile_startofpath = settings["logfilepath"];

            sql_file_select_string = "select id, source_id, sd_id, remote_url, last_revised, ";
            sql_file_select_string += " assume_complete, download_status, local_path, last_saf_id, last_downloaded, ";
            sql_file_select_string += " last_harvest_id, last_harvested, last_import_id, last_imported ";
        }

        public Source SourceParameter => source;


        public void LogParameters(Options opts)
        {
            LogHeader("Setup");
            LogLine("transfer data =  " + opts.transfer_data);
            LogLine("create core =  " + opts.create_core);
            LogLine("create json =  " + opts.create_json);
            LogLine("also do json files =  " + opts.also_do_files);
            LogLine("do statistics =  " + opts.do_statistics);
        }


        public void OpenLogFile(Options opts)
        {
            string dt_string = DateTime.Now.ToString("s", System.Globalization.CultureInfo.InvariantCulture)
                              .Replace("-", "").Replace(":", "").Replace("T", " ");
            logfile_path = logfile_startofpath + "AGGREG";
            if (opts.transfer_data) logfile_path += " -D";
            if (opts.create_core) logfile_path += " -C";
            if (opts.do_statistics) logfile_path += " -S";
            if (opts.create_json) logfile_path += " -J";
            if (opts.also_do_files) logfile_path += " -F";
            logfile_path += " " + dt_string + ".log";
            sw = new StreamWriter(logfile_path, true, System.Text.Encoding.UTF8);
        }

        public void LogLine(string message, string identifier = "")
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string feedback = dt_string + message + identifier;
            Transmit(feedback);
        }

        public void LogHeader(string message)
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string header = dt_string + "**** " + message + " ****";
            Transmit("");
            Transmit(header);
        }

        public void LogError(string message, string identifier = "")
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string error_message = dt_string + "***ERROR*** " + message;
            Transmit("");
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit(error_message);
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit("");
        }

        public void CloseLog()
        {
            LogHeader("Closing Log");
            sw.Flush();
            sw.Close();
        }


        private void Transmit(string message)
        {
            sw.WriteLine(message);
            Console.WriteLine(message);
        }

        public Source FetchSourceParameters(int source_id)
        {
            using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
            {
                source = Conn.Get<Source>(source_id);
                return source;
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
                                  has_study_contributors, has_study_references, has_study_relationships,
                                  has_object_datasets, has_object_dates, has_object_rights,
                                  has_object_relationships, has_object_pubmed_set 
                                from sf.source_parameters
                                where id > 100115
                                order by preference_rating;";

            using (var conn = new NpgsqlConnection(connString))
            {
                return conn.Query<Source>(sql_string);
            }
        }


        public string FetchConnString(string database_name)
        {
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
            builder.Host = host;
            builder.Username = user;
            builder.Password = password;
            builder.Database = database_name;
            return builder.ConnectionString;
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
            string test_string = "SELECT to_regclass('ad." + table_name + "')::varchar";
            string table_exists;
            using (var conn = new NpgsqlConnection(source_conn_string))
            {
                table_exists = conn.ExecuteScalar<string>(test_string);
            }
            if (table_exists == null)
            {
                return 0;
            }
            else
            {
                string sql_string = "SELECT count(*) from ad." + table_name;
                int? rec_num;
                using (var conn = new NpgsqlConnection(source_conn_string))
                {
                    rec_num = conn.ExecuteScalar<int?>(sql_string);
                }
                return rec_num == null ? 0 : (int)rec_num;
            }
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


        public List<AggregationObjectNum> GetObjectTypes(int aggregation_event_id)
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

            using (var conn = new NpgsqlConnection(mdr_connString))
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


        public List<StudyStudyLinkData> GetStudyStudyLinkData(int aggregation_event_id)
        {
            string sql_string = @"SELECT 
                    k.source_id, 
                    d1.default_name as source_name,
                    k.preferred_source_id as other_source_id,
                    d2.default_name as other_source_name,
                    count(preferred_sd_sid) as number_in_other_source
                    from nk.study_study_links k
                    inner join context_ctx.data_sources d1
                    on k.source_id = d1.id
                    inner join context_ctx.data_sources d2
                    on k.preferred_source_id = d2.id
                    group by source_id, preferred_source_id, d1.default_name, d2.default_name;";

            using (var conn = new NpgsqlConnection(mdr_connString))
            {
                return conn.Query<StudyStudyLinkData>(sql_string).ToList();
            }
        }

        public List<StudyStudyLinkData> GetStudyStudyLinkData2(int aggregation_event_id)
        {
            string sql_string = @"SELECT 
                    k.preferred_source_id as source_id, 
                    d2.default_name as source_name,
                    k.source_id as other_source_id,
                    d1.default_name as other_source_name,
                    count(sd_sid) as number_in_other_source
                    from nk.study_study_links k
                    inner join context_ctx.data_sources d1
                    on k.source_id = d1.id
                    inner join context_ctx.data_sources d2
                    on k.preferred_source_id = d2.id
                    group by preferred_source_id, source_id, d2.default_name, d1.default_name;;";

            using (var conn = new NpgsqlConnection(mdr_connString))
            {
                return conn.Query<StudyStudyLinkData>(sql_string).ToList();
            }
        }



        // Stores an 'extraction note', e.g. an unusual occurence found and
        // logged during the extraction, in the associated table.

        public void StoreExtractionNote(ExtractionNote ext_note)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Insert<ExtractionNote>(ext_note);
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

