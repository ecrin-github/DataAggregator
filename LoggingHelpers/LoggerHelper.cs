using Dapper;
using Npgsql;
using Serilog;
using System.Collections.Generic;
using System.Linq;

namespace DataAggregator
{
    public class LoggerHelper : ILoggerHelper
    {
        private ILogger _logger;

        public LoggerHelper(ILogger logger)
        {
            _logger = logger;
        }

        public void LogHeader(string header_text)
        {
            _logger.Information("");
            _logger.Information(header_text.ToUpper());
            _logger.Information("");
        }


        public void LogParameters(Options opts)
        {
            LogHeader("Setup");
            _logger.Information("transfer data =  " + opts.transfer_data);
            _logger.Information("create core =  " + opts.create_core);
            _logger.Information("create json =  " + opts.create_json);
            _logger.Information("also do json files =  " + opts.also_do_files);
            _logger.Information("do statistics =  " + opts.do_statistics);
        }



        public void LogStudyHeader(Options opts, string dbline)
        {
            /*
            int harvest_type = opts.harvest_type_id;
            string dividerline = "";
            if (opts.harvest_all_test_data || opts.setup_expected_data_only)
            {
                dividerline = new string('-', 70);
            }
            else
            {
                dividerline = (harvest_type == 1 || harvest_type == 2) ? new string('=', 70) : new string('-', 70);
            }
            _logger.Information("");
            _logger.Information(dividerline);
            _logger.Information(dbline);
            _logger.Information(dividerline);
            _logger.Information("");
            */
        }

        /*
        public void LogTableStatistics(ISource s, string schema)
        {
            // Gets and logs record count for each table in the sd schema of the database
            // Start by obtaining conection string, then construct log line for each by 
            // calling db interrogation for each applicable table
            string db_conn = s.db_conn;

            _logger.Information("");
            _logger.Information("TABLE RECORD NUMBERS");

            if (s.has_study_tables)
            {
                _logger.Information("");
                _logger.Information("study tables...\n");
                _logger.Information(GetTableRecordCount(db_conn, schema, "studies"));
                _logger.Information(GetTableRecordCount(db_conn, schema, "study_identifiers"));
                _logger.Information(GetTableRecordCount(db_conn, schema, "study_titles"));

                // these are database dependent
                if (s.has_study_topics) _logger.Information(GetTableRecordCount(db_conn, schema, "study_topics"));
                if (s.has_study_features) _logger.Information(GetTableRecordCount(db_conn, schema, "study_features"));
                if (s.has_study_contributors) _logger.Information(GetTableRecordCount(db_conn, schema, "study_contributors"));
                if (s.has_study_references) _logger.Information(GetTableRecordCount(db_conn, schema, "study_references"));
                if (s.has_study_relationships) _logger.Information(GetTableRecordCount(db_conn, schema, "study_relationships"));
                if (s.has_study_links) _logger.Information(GetTableRecordCount(db_conn, schema, "study_links"));
                if (s.has_study_ipd_available) _logger.Information(GetTableRecordCount(db_conn, schema, "study_ipd_available"));

                _logger.Information(GetTableRecordCount(db_conn, schema, "study_hashes"));
                IEnumerable<hash_stat> study_hash_stats = (GetHashStats(db_conn, schema, "study_hashes"));
                if (study_hash_stats.Count() > 0)
                {
                    _logger.Information("");
                    _logger.Information("from the hashes...\n");
                    foreach (hash_stat hs in study_hash_stats)
                    {
                        _logger.Information(hs.num.ToString() + " study records have " + hs.hash_type + " (" + hs.hash_type_id.ToString() + ")");
                    }
                }
            }
            _logger.Information("");
            _logger.Information("object tables...\n");
            // these common to all databases
            _logger.Information(GetTableRecordCount(db_conn, schema, "data_objects"));
            _logger.Information(GetTableRecordCount(db_conn, schema, "object_instances"));
            _logger.Information(GetTableRecordCount(db_conn, schema, "object_titles"));

            // these are database dependent		

            if (s.has_object_datasets) _logger.Information(GetTableRecordCount(db_conn, schema, "object_datasets"));
            if (s.has_object_dates) _logger.Information(GetTableRecordCount(db_conn, schema, "object_dates"));
            if (s.has_object_relationships) _logger.Information(GetTableRecordCount(db_conn, schema, "object_relationships"));
            if (s.has_object_rights) _logger.Information(GetTableRecordCount(db_conn, schema, "object_rights"));
            if (s.has_object_pubmed_set)
            {
                _logger.Information(GetTableRecordCount(db_conn, schema, "journal_details"));
                _logger.Information(GetTableRecordCount(db_conn, schema, "object_contributors"));
                _logger.Information(GetTableRecordCount(db_conn, schema, "object_topics"));
                _logger.Information(GetTableRecordCount(db_conn, schema, "object_comments"));
                _logger.Information(GetTableRecordCount(db_conn, schema, "object_descriptions"));
                _logger.Information(GetTableRecordCount(db_conn, schema, "object_identifiers"));
                _logger.Information(GetTableRecordCount(db_conn, schema, "object_db_links"));
                _logger.Information(GetTableRecordCount(db_conn, schema, "object_publication_types"));
            }

            _logger.Information(GetTableRecordCount(db_conn, schema, "object_hashes"));
            IEnumerable<hash_stat> object_hash_stats = (GetHashStats(db_conn, schema, "object_hashes"));
            if (object_hash_stats.Count() > 0)
            {
                _logger.Information("");
                _logger.Information("from the hashes...\n");
                foreach (hash_stat hs in object_hash_stats)
                {
                    _logger.Information(hs.num.ToString() + " object records have " + hs.hash_type + " (" + hs.hash_type_id.ToString() + ")");
                }
            }
        }
  */

        private string GetTableRecordCount(string db_conn, string schema, string table_name)
        {
            string sql_string = "select count(*) from " + schema + "." + table_name;

            using (NpgsqlConnection conn = new NpgsqlConnection(db_conn))
            {
                int res = conn.ExecuteScalar<int>(sql_string);
                return res.ToString() + " records found in " + schema + "." + table_name;
            }
        }

        /*
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
