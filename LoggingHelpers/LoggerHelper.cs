using Dapper;
using Npgsql;
using System.IO;
using Microsoft.Extensions.Configuration;
using System;

namespace DataAggregator
{
    public class LoggingHelper
    {
        private string logfile_startofpath;
        private string logfile_path;
        private StreamWriter sw;

        public LoggingHelper()
        {
            IConfigurationRoot settings = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            logfile_startofpath = settings["logfilepath"];

            string dt_string = DateTime.Now.ToString("s", System.Globalization.CultureInfo.InvariantCulture)
                              .Replace(":", "").Replace("T", " ");

            string log_folder_path = Path.Combine(logfile_startofpath, "aggs");
            if (!Directory.Exists(log_folder_path))
            {
                Directory.CreateDirectory(log_folder_path);
            }

            logfile_path = Path.Combine(log_folder_path, "AGG " + dt_string + ".log");
            sw = new StreamWriter(logfile_path, true, System.Text.Encoding.UTF8);
        }


        public LoggingHelper(string logFilePath)
        {
            sw = new StreamWriter(logFilePath, true, System.Text.Encoding.UTF8);
        }


        public string LogFilePath => logfile_path;

        public void LogLine(string message, string identifier = "")
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string feedback = dt_string + message + identifier;
            Transmit(feedback);
        }


        public void LogStudyHeader(bool using_test_data, string dbline)
        {
            string dividerline = using_test_data ? new string('-', 70) : new string('=', 70);
            LogLine("");
            LogLine(dividerline);
            LogLine(dbline);
            LogLine(dividerline);
            LogLine("");
        }


        public void LogHeader(string message)
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string header = dt_string + "**** " + message.ToUpper() + " ****";
            Transmit("");
            Transmit(header);
        }


        public void LogError(string message)
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string error_message = dt_string + "***ERROR*** " + message;
            Transmit("");
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit(error_message);
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit("");
        }


        public void LogCodeError(string header, string errorMessage, string stackTrace)
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string headerMessage = dt_string + "***ERROR*** " + header + "\n";
            Transmit("");
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit(headerMessage);
            Transmit(errorMessage + "\n");
            Transmit(stackTrace);
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit("");
        }


        public void LogParseError(string header, string errorNum, string errorType)
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string error_message = dt_string + "***ERROR*** " + "Error " + errorNum + ": " + header + " " + errorType;
            Transmit(error_message);
        }

        public void SpacedInformation(string header_text)
        {
            LogLine("");
            LogLine(header_text);
        }


        public void LogParameters(Options opts)
        {
            LogHeader("Setup");
            LogLine("transfer data =  " + opts.transfer_data);
            LogLine("create core =  " + opts.create_core);
            LogLine("create json =  " + opts.create_json);
            LogLine("also do json files =  " + opts.also_do_files);
            LogLine("do statistics =  " + opts.do_statistics);
        }


        public void Reattach()
        {
            sw = new StreamWriter(logfile_path, true, System.Text.Encoding.UTF8);
        }


        public void SwitchLog()
        {
            LogHeader("Switching Log File Control");
            sw.Flush();
            sw.Close();
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


        /*
        public void LogTableStatistics(ISource s, string schema)
        {
            // Gets and logs record count for each table in the sd schema of the database
            // Start by obtaining conection string, then construct log line for each by 
            // calling db interrogation for each applicable table
            string db_conn = s.db_conn;

            _loggingHelper.Information("");
            _loggingHelper.Information("TABLE RECORD NUMBERS");

            if (s.has_study_tables)
            {
                _loggingHelper.Information("");
                _loggingHelper.Information("study tables...\n");
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "studies"));
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "study_identifiers"));
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "study_titles"));

                // these are database dependent
                if (s.has_study_topics) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "study_topics"));
                if (s.has_study_features) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "study_features"));
                if (s.has_study_contributors) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "study_contributors"));
                if (s.has_study_references) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "study_references"));
                if (s.has_study_relationships) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "study_relationships"));
                if (s.has_study_links) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "study_links"));
                if (s.has_study_ipd_available) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "study_ipd_available"));

                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "study_hashes"));
                IEnumerable<hash_stat> study_hash_stats = (GetHashStats(db_conn, schema, "study_hashes"));
                if (study_hash_stats.Count() > 0)
                {
                    _loggingHelper.Information("");
                    _loggingHelper.Information("from the hashes...\n");
                    foreach (hash_stat hs in study_hash_stats)
                    {
                        _loggingHelper.Information(hs.num.ToString() + " study records have " + hs.hash_type + " (" + hs.hash_type_id.ToString() + ")");
                    }
                }
            }
            _loggingHelper.Information("");
            _loggingHelper.Information("object tables...\n");
            // these common to all databases
            _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "data_objects"));
            _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_instances"));
            _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_titles"));

            // these are database dependent		

            if (s.has_object_datasets) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_datasets"));
            if (s.has_object_dates) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_dates"));
            if (s.has_object_relationships) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_relationships"));
            if (s.has_object_rights) _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_rights"));
            if (s.has_object_pubmed_set)
            {
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "journal_details"));
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_contributors"));
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_topics"));
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_comments"));
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_descriptions"));
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_identifiers"));
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_db_links"));
                _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_publication_types"));
            }

            _loggingHelper.Information(GetTableRecordCount(db_conn, schema, "object_hashes"));
            IEnumerable<hash_stat> object_hash_stats = (GetHashStats(db_conn, schema, "object_hashes"));
            if (object_hash_stats.Count() > 0)
            {
                _loggingHelper.Information("");
                _loggingHelper.Information("from the hashes...\n");
                foreach (hash_stat hs in object_hash_stats)
                {
                    _loggingHelper.Information(hs.num.ToString() + " object records have " + hs.hash_type + " (" + hs.hash_type_id.ToString() + ")");
                }
            }
        }


        private string GetTableRecordCount(string db_conn, string schema, string table_name)
        {
            string sql_string = "select count(*) from " + schema + "." + table_name;

            using (NpgsqlConnection conn = new NpgsqlConnection(db_conn))
            {
                int res = conn.ExecuteScalar<int>(sql_string);
                return res.ToString() + " records found in " + schema + "." + table_name;
            }
        }


        private IEnumerable<hash_stat> GetHashStats(string db_conn, string schema, string table_name)
        {
            string sql_string = "select hash_type_id, hash_type, count(id) as num from " + schema + "." + table_name;
            sql_string += " group by hash_type_id, hash_type order by hash_type_id;";

            using (NpgsqlConnection conn = new NpgsqlConnection(db_conn))
            {
                return conn.Query<hash_stat>(sql_string);
            }
        }
        */
    }
}
