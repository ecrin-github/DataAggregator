﻿using Dapper;
using Npgsql;
using System;
using Serilog;

namespace DataAggregator
{
    public class DBUtilities
    {
        string connstring;
        ILogger _logger;

        public DBUtilities(string _connstring, ILogger logger)
        {
            connstring = _connstring;
            _logger = logger;
        }

        public int ExecuteSQL(string sql_string)
        {
            using (var conn = new NpgsqlConnection(connstring))
            {
                try
                {
                    return conn.Execute(sql_string);
                }
                catch (Exception e)
                {
                    _logger.Error("In ExecuteSQL; " + e.Message + ", \nSQL was: " + sql_string);
                    return 0;
                }
            }
        }
        
        public int GetMaxId(string schema_name, string table_name)
        {
            string sql_string = @"select max(id) from " + schema_name + "." + table_name;
            using (var conn = new NpgsqlConnection(connstring))
            {
               return conn.ExecuteScalar<int>(sql_string);
            }
        }


        public int GetAggMinId(string full_table_name)
        {
            string sql_string = @"select min(id) from " + full_table_name;
            using (var conn = new NpgsqlConnection(connstring))
            {
                return conn.ExecuteScalar<int>(sql_string);
            }
        }

        public int GetAggMaxId(string full_table_name)
        {
            string sql_string = @"select max(id) from " + full_table_name;
            using (var conn = new NpgsqlConnection(connstring))
            {
                return conn.ExecuteScalar<int>(sql_string);
            }
        }

        public int GetCount(string table_name)
        {
            string sql_string = @"SELECT COUNT(*) FROM " + table_name; 

            using (var conn = new NpgsqlConnection(connstring))
            {
                return conn.ExecuteScalar<int>(sql_string);
            }
        }


        public void Update_SourceTable_ExportDate(string schema_name, string table_name)
        {
            try
            {
                int rec_count = GetMaxId(schema_name, table_name);
                int rec_batch = 50000;
                string sql_string = @"UPDATE " + schema_name + "." + table_name + @" s
                                      SET exported_on = CURRENT_TIMESTAMP ";

                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " where s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Updated " + schema_name + "." + table_name + " export date, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                        _logger.Information(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(sql_string);
                    _logger.Information("Updated " + schema_name + "." + table_name + " export date, as a single batch");
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In update export date (" + schema_name + "." + table_name + ") to aggregate table: " + res);
            }
        }


        public int ExecuteTransferSQL(string sql_string, string schema_name, string table_name, string context)
        {
            try
            {
                int transferred = 0;
                int rec_count = GetMaxId(schema_name, table_name);
                int rec_batch = 50000;
                // int rec_batch = 10000;  // for testing 
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();
                        transferred += ExecuteSQL(batch_sql_string);

                        string feedback = "Transferred " + schema_name + "." + table_name + " (" + context + ") data, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                        _logger.Information(feedback);
                    }
                }
                else
                {
                    transferred = ExecuteSQL(sql_string);
                    _logger.Information("Transferred " + schema_name + "." + table_name + " (" + context + ") data, as a single batch");
                }
                return transferred;
            }

            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In data transfer (" + schema_name + "." + table_name + "(" + context + ")) to aggregate table: " + res);
                return 0;
            }
        }


        public int  ExecuteCoreTransferSQL(string sql_string, string full_table_name, string dest_table_name = "")
        {
            try
            {
                int transferred = 0;
                int min_id = GetAggMinId(full_table_name);
                int max_id = GetAggMaxId(full_table_name);
                int rec_batch = 50000;
                string feedback = "";
                
                // int rec_batch = 10000;  // for testing 
                if (max_id - min_id > rec_batch)
                {
                    for (int r = min_id; r <= max_id; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " where id >= " + r.ToString() + " and id < " + (r + rec_batch).ToString();
                        transferred += ExecuteSQL(batch_sql_string);

                        feedback = "Transferred " + full_table_name + " data, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < max_id) ? (r + rec_batch - 1).ToString() : max_id.ToString();
                        feedback += dest_table_name == "" ? "" : ", to " + dest_table_name;
                        _logger.Information(feedback);
                    }
                }
                else
                {
                    transferred = ExecuteSQL(sql_string);
                    feedback = dest_table_name == "" ? "Transferred " + full_table_name + " data, as a single batch"
                               : "Transferred " + full_table_name + " data, to " + dest_table_name + " as a single batch";
                    _logger.Information(feedback);

                }
                return transferred;
            }

            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In data transfer (" + full_table_name + " to core table: " + res);
                return 0;
            }
        }


        public int ExecuteCoreSearchSQL(string sql_string, string data_type, int min_id, int max_id)
        {
            try
            {
                int transferred = 0;
                int rec_batch = 50000;
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();
                    transferred += ExecuteSQL(batch_sql_string);

                    string feedback = "Updated study_search table with " + data_type + " data, " + r.ToString() + " to ";
                    feedback += (r + rec_batch < max_id) ? (r + rec_batch - 1).ToString() : max_id.ToString();
                    _logger.Information(feedback);
                }
                return transferred;
            }

            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In study search update (" + data_type + "): " + res);
                return 0;
            }
        }



        public int ExecuteCoreSearchByStudySQL(string sql_string, string data_type, int min_id, int max_id)
        {
            // usesd study id to go through records becasue records n=must be grouped by study
            try
            {
                int transferred = 0;
                int rec_batch = 10000;
                for (int r = min_id; r <= max_id; r += rec_batch)
                {
                    string batch_sql_string = sql_string + " and s.study_id >= " + r.ToString() + " and s.study_id < " + (r + rec_batch).ToString();
                    transferred += ExecuteSQL(batch_sql_string);

                    string feedback = "Updated study_search table with " + data_type + " data, " + r.ToString() + " to ";
                    feedback += (r + rec_batch < max_id) ? (r + rec_batch - 1).ToString() : max_id.ToString();
                    _logger.Information(feedback);
                }
                return transferred;
            }

            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In study search update (" + data_type + "): " + res);
                return 0;
            }
        }




        public int ExecuteCoreSearchObjectSQL(string where_string, string object_type)
        {
            try
            {
                int transferred = 0;
                int rec_batch = 50000;

                string setup_sql = @"TRUNCATE TABLE core.temp_searchobjects RESTART IDENTITY;
                            INSERT INTO core.temp_searchobjects(study_id)
                            SELECT DISTINCT k.study_id from core.study_object_links k
                            inner join core.data_objects b
                            on k.object_id = b.id
                            where " + where_string;
                ExecuteSQL(setup_sql);

                string sql_string = @"UPDATE core.study_search ss
                            SET has_" + object_type + @" = true
                            FROM core.temp_searchobjects d
                            WHERE ss.id = d.study_id ";

                int recs_in_table = GetCount("core.temp_searchobjects");
                if (recs_in_table > rec_batch)
                {
                    for (int r = 1; r <= recs_in_table; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and d.id >= " + r.ToString() + " and d.id < " + (r + rec_batch).ToString();
                        transferred += ExecuteSQL(batch_sql_string);

                        string feedback = "Updated study_search table with has_" + object_type + " data, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < recs_in_table) ? (r + rec_batch - 1).ToString() : recs_in_table.ToString();
                        _logger.Information(feedback);
                    }
                }
                else
                {
                    transferred = ExecuteSQL(sql_string);
                    _logger.Information("Updated study_search table with has_" + object_type + " data, as a single batch");
                }
                return transferred;
            }

            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In study search update (has_" + object_type + " to core table: " + res);
                return 0;
            }
        }

        public int ExecuteProvenanceSQL(string sql_string, string full_table_name)
        {
            try
            {
                int transferred = 0;
                int min_id = GetAggMinId(full_table_name);
                int max_id = GetAggMaxId(full_table_name);
                int rec_batch = 50000;
                if (max_id - min_id > rec_batch)
                {
                    for (int r = min_id; r <= max_id; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " AND s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();
                        transferred += ExecuteSQL(batch_sql_string);

                        string feedback = "Updated " + full_table_name + " with provenance data, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < max_id) ? (r + rec_batch - 1).ToString() : max_id.ToString();
                        _logger.Information(feedback);
                    }
                }
                else
                {
                    transferred = ExecuteSQL(sql_string);
                    _logger.Information("Updated " + full_table_name + " with provenance data, as a single batch");
                }
                return transferred;
            }
            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In updating provenance data in " + full_table_name + ": " + res);
                return 0;
            }
        }
    }
}
