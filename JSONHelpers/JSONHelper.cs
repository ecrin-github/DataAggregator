using Newtonsoft.Json;
using System;
using System.Collections.Generic;
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
              , json                     INT             NULL
             );
             CREATE INDEX studies_json_id ON core.studies_json(id);";
            db.ExecuteSQL(sql_string);

            sql_string = @"DROP TABLE IF EXISTS core.objects_json(
                CREATE TABLE nk.studies_json(
                id                       INT             NOT NULL PRIMARY KEY
              , json                     INT             NULL
             );
             CREATE INDEX objects_json_id ON core.objects_json(id);";
            db.ExecuteSQL(sql_string);
        }


        public void LoopThroughStudyRecords(JSONDataLayer repo, int min_id, int max_id)
        {
            JSONStudyProcessor processor = new JSONStudyProcessor(repo);

            // Do 10,000 ids at a time
            int batch = 10000;

            for (int n = min_id; n <= max_id; n+= batch)
            {
                IEnumerable<int> id_numbers = repo.FetchIds("studies", n, batch);
                foreach (int id in id_numbers)
                {
                    // Construct single study object, drawing data from various database tables 
                    // and serialise to a formatted json string, then store json in the database.

                    JSONStudy obj = processor.CreateStudyObject(id);
                    if (obj != null)
                    {
                        var formatted_json = JsonConvert.SerializeObject(obj, Formatting.Indented);
                        // may need to read this into a stream...
                        //repo.StoreStudyJSON();
                    }

                    if (n % 100 == 0) StringHelpers.SendFeedback(n.ToString() + "records preocessed");
                }
            }
        }
    }
}
