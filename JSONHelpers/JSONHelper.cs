using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DataAggregator
{
    public class JSONHelper
    {
        string connString;
        DBUtilities db;

        public JSONHelper(string _connString)
        {
            connString = _connString;
            db = new DBUtilities(connString);
        }

        public void CreateJSONTables()
        {
            string sql_string = @"DROP TABLE IF EXISTS core.study_json(
                CREATE TABLE nk.studies_json(
                id                       INT             NOT NULL PRIMARY KEY
              , json                     VARCHAR         NULL
             );
             CREATE INDEX studies_json_id ON core.studies_json(id);";
            db.ExecuteSQL(sql_string);

            sql_string = @"DROP TABLE IF EXISTS core.objects_json(
                CREATE TABLE nk.objects_json(
                id                       INT             NOT NULL PRIMARY KEY
              , json                     VARCHAR         NULL
             );
            CREATE INDEX objects_json_id ON core.objects_json(id);";
            db.ExecuteSQL(sql_string);
        }


        public void CreateJSONStudyData(bool also_do_files)
        {
            JSONDataLayer repo = new JSONDataLayer();
            int min_id = repo.FetchMinId("studies");
            int max_id = repo.FetchMaxId("studies");

            LoopThroughStudyRecords(repo, min_id, max_id, also_do_files);
        }


        public void CreateJSONObjectData(bool also_do_files)
        {
            JSONDataLayer repo = new JSONDataLayer();
            int min_id = repo.FetchMinId("studies");
            int max_id = repo.FetchMaxId("studies");

            LoopThroughObjectRecords(repo, min_id, max_id, also_do_files);
        }


        public void LoopThroughStudyRecords(JSONDataLayer repo, int min_id, int max_id, bool also_do_files)
        {
            JSONStudyProcessor processor = new JSONStudyProcessor(repo);

            // Do 10,000 ids at a time
            int batch = 10000;
            string folder_path = "";
            if (also_do_files)
            {
                // Create folder for the next batch, obtaining the parent path from repo

                string folder_name = "studies " + min_id.ToString() + " to " + (min_id + batch - 1).ToString();
                folder_path = Path.Combine(repo.FolderBase, folder_name);
                Directory.CreateDirectory(folder_path);
            }

            for (int n = min_id; n <= max_id; n+= batch)
            {
                IEnumerable<int> id_numbers = repo.FetchIds("studies", n, batch);
                foreach (int id in id_numbers)
                {
                    // Construct single study object, drawing data from various database tables 
                    // and serialise to a formatted json string, then store json in the database.

                    JSONStudy st = processor.CreateStudyObject(id);
                    if (st != null)
                    {
                        var formatted_json = JsonConvert.SerializeObject(st, Formatting.Indented);
                        repo.StoreStudyJSON(new DBStudyJSON(id, formatted_json));
                        if (also_do_files)
                        {
                            string file_name = "study " + id.ToString() + ".json";
                            string full_path = Path.Combine(folder_path, file_name);
                            File.WriteAllText(full_path, formatted_json);
                        }
                    }

                    if (n % 100 == 0) StringHelpers.SendFeedback(n.ToString() + "records preocessed");
                }
            }
        }


        public void LoopThroughObjectRecords(JSONDataLayer repo, int min_id, int max_id, bool also_do_files)
        {
            JSONStudyProcessor processor = new JSONStudyProcessor(repo);

            // Do 10,000 ids at a time
            int batch = 10000;
            string folder_path = "";
            for (int n = min_id; n <= max_id; n += batch)
            {
                if (also_do_files)
                {
                    // Create folder for the next batch, obtaining the parent path from repo

                    string folder_name = "objects " + min_id.ToString() + " to " + (min_id + batch -1).ToString();
                    folder_path = Path.Combine(repo.FolderBase, folder_name);
                    Directory.CreateDirectory(folder_path);
                }

                IEnumerable<int> id_numbers = repo.FetchIds("studies", n, batch);
                foreach (int id in id_numbers)
                {
                    // Construct single study object, drawing data from various database tables 
                    // and serialise to a formatted json string, then store json in the database.

                    JSONStudy obj = processor.CreateStudyObject(id);
                    if (obj != null)
                    {
                        var formatted_json = JsonConvert.SerializeObject(obj, Formatting.Indented);
                        repo.StoreObjectJSON(new DBObjectJSON(id, formatted_json));
                        // may need to read this into a stream...
                        //repo.StoreStudyJSON();
                        if (also_do_files)
                        {
                            string file_name = "object " + id.ToString() + ".json";
                            string full_path = Path.Combine(folder_path, file_name);
                            File.WriteAllText(full_path, formatted_json);
                        }
                    }

                    if (n % 100 == 0) StringHelpers.SendFeedback(n.ToString() + "records preocessed");
                }
            }
        }
    }
}



