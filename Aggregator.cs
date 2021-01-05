using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DataAggregator
{
    public class Aggregator
    {
        int agg_event_id;
        LoggingDataLayer logging_repo;

        public Aggregator(LoggingDataLayer _logging_repo)
        {
            logging_repo = _logging_repo;
            agg_event_id = logging_repo.GetNextAggEventId();
        }

        public async Task AggregateDataAsync(Options opts)
        {
            logging_repo.LogParameters(opts);
            DataLayer repo = new DataLayer("mdr");

            // set up the context DB as two sets of foreign tables
            // as it is used in several places
            repo.SetUpTempContextFTWs();


            if (opts.transfer_data)
            {
                // Establish the mdr and logging repo layers.
                logging_repo.LogHeader("Establish aggregate schemas");

                // In the mdr database, establish new tables, 
                // for the three schemas st, ob, nk (schemas should already exist)
                SchemaBuilder sb = new SchemaBuilder(repo.ConnString, logging_repo);
                sb.DeleteStudyTables();
                sb.DeleteObjectTables();
                sb.DeleteLinkTables();

                sb.BuildNewStudyTables();
                sb.BuildNewObjectTables();
                sb.BuildNewLinkTables();
                logging_repo.LogLine("Tables created");

                // construct the aggregation event record
                AggregationEvent agg_event = new AggregationEvent(agg_event_id);

                // Derive a new table of inter-study relationships -
                // First get a list of all the study sources and
                // ensure it is sorted correctly.

                IEnumerable<Source> sources = logging_repo.RetrieveDataSources()
                                           .OrderBy(s => s.preference_rating);
                logging_repo.LogLine("Sources obtained");

                StudyLinkBuilder slb = new StudyLinkBuilder(repo, logging_repo);
                slb.CollectStudyStudyLinks(sources);
                slb.ProcessStudyStudyLinks();
                logging_repo.LogLine("Study-study links identified");

                // Start the data transfer process
                logging_repo.LogHeader("Data Transfer");

                // Loop through the study sources (in preference order)
                // In each case establish and then drop the source tables 
                // in a foreign table wrapper
                int num_studies_imported = 0;
                int num_objects_imported = 0;
        
                foreach (Source s in sources)
                {
                    string schema_name = repo.SetUpTempFTW(s.database_name);
                    string conn_string = logging_repo.FetchConnString(s.database_name);
                    DataTransferBuilder tb = new DataTransferBuilder(s, schema_name, conn_string, logging_repo);
                    if (s.has_study_tables)
                    {
                        tb.ProcessStudyIds();
                        num_studies_imported += tb.TransferStudyData();
                        tb.ProcessStudyObjectIds();
                    }
                    else
                    {
                        tb.ProcessStandaloneObjectIds();
                    }
                    num_objects_imported += tb.TransferObjectData();
                    repo.DropTempFTW(s.database_name);
                }

                // Also use the study groups to set up study_relationship records
                slb.CreateStudyGroupRecords();

                // Update aggregation event record.

                agg_event.num_studies_imported = num_studies_imported;
                agg_event.num_objects_imported = num_objects_imported;

                string mdr_string = logging_repo.FetchConnString("mdr");
                agg_event.num_total_studies = logging_repo.GetAggregateRecNum("studies", "st", mdr_string);
                agg_event.num_total_objects = logging_repo.GetAggregateRecNum("data_objects", "ob", mdr_string);
                agg_event.num_total_study_object_links = logging_repo.GetAggregateRecNum("all_ids_data_objects", "nk", mdr_string);

                logging_repo.StoreAggregationEvent(agg_event);
                repo.DropTempContextFTWs();
            }
            

            if (opts.create_core)
            {
                // create core tables
                SchemaBuilder sb = new SchemaBuilder(repo.ConnString, logging_repo);
                logging_repo.LogHeader("Set up");
                sb.DeleteCoreTables();
                sb.BuildNewCoreTables();

                // transfer data to core tables
                DataTransferBuilder tb = new DataTransferBuilder(logging_repo);
                logging_repo.LogHeader("Transferring study data");
                tb.TransferCoreStudyData();
                logging_repo.LogHeader("Transferring object data");
                tb.TransferCoreObjectData();
                logging_repo.LogHeader("Transferring link data");
                tb.TransferCoreLinkData();

                // Include generation of data provenance strings
                // Need an additional temporary FTW link to mon
                logging_repo.LogHeader("Finish");
                repo.SetUpTempFTW("mon");
                tb.GenerateProvenanceData();
                repo.DropTempFTW("mon");
            }


            if (opts.do_statistics)
            {
                int last_agg_event_id = logging_repo.GetLastAggEventId();
                StatisticsBuilder stb = new StatisticsBuilder(last_agg_event_id, logging_repo);
                stb.GetStatisticsBySource();
                stb.GetSummaryStatistics();
            }


            if (opts.create_json)
            {
                string conn_string = logging_repo.FetchConnString("mdr");
                JSONHelper jh = new JSONHelper(conn_string, logging_repo);

                // Create json fields.

                // if tables are to be left as they are, add false as 
                // an additional boolean (default = true)
                // if tables are to have further data appended add an integer
                // offset that represents the records to skip (default = 0)

                logging_repo.LogHeader("Creating JSON study data");
                jh.CreateJSONStudyData(opts.also_do_files);
                //jh.UpdateJSONStudyData(opts.also_do_files);
                logging_repo.LogHeader("Creating JSON object data");
                jh.CreateJSONObjectData(opts.also_do_files);
                //jh.UpdateJSONObjectData(opts.also_do_files);
            }

            repo.DropTempContextFTWs();
            logging_repo.CloseLog();
        }
    }
}
