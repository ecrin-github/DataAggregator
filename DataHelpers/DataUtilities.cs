using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class DBUtilities
    {
        string connstring;

        public DBUtilities(string _connstring)
        {
            connstring = _connstring;
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
                    StringHelpers.SendError("In ExecuteSQL; " + e.Message + ", \nSQL was: " + sql_string);
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


        public int GetAggMaxId(string table_name, int offset)
        {
            // TO DO
            string sql_string = @"select max(id) from " + table_name;
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
                        StringHelpers.SendFeedback(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(sql_string);
                    StringHelpers.SendFeedback("Updated " + schema_name + "." + table_name + " export date, as a single batch");
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                StringHelpers.SendError("In update export date (" + schema_name + "." + table_name + ") to aggregate table: " + res);
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
                        StringHelpers.SendFeedback(feedback);
                    }
                }
                else
                {
                    transferred = ExecuteSQL(sql_string);
                    StringHelpers.SendFeedback("Transferred " + schema_name + "." + table_name + " (" + context + ") data, as a single batch");
                }
                return transferred;
            }
            

            catch (Exception e)
            {
                string res = e.Message;
                StringHelpers.SendError("In data transfer (" + schema_name + "." + table_name + "(" + context + ")) to aggregate table: " + res);
                return 0;
            }
        }


        public int  ExecuteCoreTransferSQL(string sql_string, string table_name, int offset)
        {
            try
            {
                int transferred = 0;
                int rec_count = GetAggMaxId(table_name, offset);
                int rec_batch = 50000;
                // int rec_batch = 10000;  // for testing 
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string batch_sql_string = sql_string + " and s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();
                        transferred += ExecuteSQL(batch_sql_string);

                        string feedback = "Transferred " + table_name + " data, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                        StringHelpers.SendFeedback(feedback);
                    }
                }
                else
                {
                    transferred = ExecuteSQL(sql_string);
                    StringHelpers.SendFeedback("Transferred " + table_name + " data, as a single batch");
                }
                return transferred;
            }


            catch (Exception e)
            {
                string res = e.Message;
                StringHelpers.SendError("In data transfer (" + table_name + " to core table: " + res);
                return 0;
            }
        }
    }
}
