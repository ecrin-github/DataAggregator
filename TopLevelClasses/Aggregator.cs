using System.Collections.Generic;
using System.Linq;
using Serilog;
using System;

namespace DataAggregator
{
    public class Aggregator : IAggregator
    {
        ILogger _logger;
        ILoggerHelper _logger_helper;
        IMonitorDataLayer _mon_repo;
        ITestingDataLayer _test_repo;
        ICredentials _credentials;

        int agg_event_id;

        public Aggregator(ILogger logger, ILoggerHelper logger_helper,
                         IMonitorDataLayer mon_repo, ITestingDataLayer test_repo,
                         ICredentials credentials)
        {
            _logger = logger;
            _logger_helper = logger_helper;
            _mon_repo = mon_repo;
            _test_repo = test_repo;
            _credentials = credentials;

            agg_event_id = _mon_repo.GetNextAggEventId();
        }
        

        public int AggregateData(Options opts)
        {
            try
            {
                _logger_helper.LogParameters(opts);

                // set up the context DB as two sets of foreign tables
                // as it is used in several places

                _mon_repo.SetUpTempContextFTWs(_credentials);

                if (opts.transfer_data)
                {
                    // In the mdr database, establish new tables, 
                    // for the three schemas st, ob, nk (schemas should already exist)

                    _logger_helper.LogHeader("Establishing aggregate schemas");
                    string mdr_conn_string = _credentials.GetConnectionString("mdr", false);
                    SchemaBuilder sb = new SchemaBuilder(mdr_conn_string);
                    sb.DeleteStudyTables();
                    sb.DeleteObjectTables();
                    sb.DeleteLinkTables();

                    sb.BuildNewStudyTables();
                    sb.BuildNewObjectTables();
                    sb.BuildNewLinkTables();
                    _logger.Information("Study, object and link aggregate tables recreated");

                    // construct the aggregation event record
                    AggregationEvent agg_event = new AggregationEvent(agg_event_id);

                    // Derive a new table of inter-study relationships -
                    // First get a list of all the study sources and
                    // ensure it is sorted correctly.

                    IEnumerable<Source> sources = _mon_repo.RetrieveDataSources()
                                               .OrderBy(s => s.preference_rating);
                    _logger.Information("Sources obtained");

                    // Then use the study link builder to create
                    // a record of all current study - study links

                    StudyLinkBuilder slb = new StudyLinkBuilder(_credentials);
                    slb.CollectStudyStudyLinks(sources);
                    slb.ProcessStudyStudyLinks();
                    _logger.Information("Study-study links identified");

                    // Start the data transfer process
                    _logger_helper.LogHeader("Data Transfer");

                    // Loop through the study sources (in preference order)
                    // In each case establish and then drop the source tables   
                    // in a foreign table wrapper

                    int num_studies_imported = 0;
                    int num_objects_imported = 0;

                    foreach (Source source in sources)
                    {
                        // schema name is the ad tables in a FTW - i.e. <db name>_ad
                        // also use credentials here to get the connection string for the source database

                        string schema_name = _mon_repo.SetUpTempFTW(_credentials, source.database_name);
                        string source_conn_string = _credentials.GetConnectionString(source.database_name, false);

                        DataTransferBuilder tb = new DataTransferBuilder(source, schema_name, 
                            source_conn_string, mdr_conn_string, _logger);
                        if (source.has_study_tables)
                        {
                            tb.ProcessStudyIds();
                            num_studies_imported += tb.TransferStudyData();
                            tb.ProcessStudyObjectIds();
                        }
                        else
                        {
                            tb.ProcessStandaloneObjectIds(sources);
                        }
                        num_objects_imported += tb.TransferObjectData();
                        _mon_repo.DropTempFTW(source.database_name);
                    }

                    // Also use the study groups data to insert additional study_relationship records
                    slb.CreateStudyGroupRecords();

                    // Update aggregation event record.

                    agg_event.num_studies_imported = num_studies_imported;
                    agg_event.num_objects_imported = num_objects_imported;

                    agg_event.num_total_studies = _mon_repo.GetAggregateRecNum("studies", "st", mdr_conn_string);
                    agg_event.num_total_objects = _mon_repo.GetAggregateRecNum("data_objects", "ob", mdr_conn_string);
                    agg_event.num_total_study_object_links = _mon_repo.GetAggregateRecNum("all_ids_data_objects", "nk", mdr_conn_string);

                    _mon_repo.StoreAggregationEvent(agg_event);
                }


                if (opts.create_core)
                {
                    // create core tables
                    string mdr_conn_string = _credentials.GetConnectionString("mdr", false);
                    CoreBuilder cb = new CoreBuilder(mdr_conn_string);
                    _logger_helper.LogHeader("Set up");
                    cb.DeleteCoreTables();
                    _logger.Information("Core tables dropped");
                    cb.BuildNewCoreTables();
                    _logger.Information("Core tables created");

                    // transfer data to core tables
                    CoreTransferBuilder ctb = new CoreTransferBuilder(mdr_conn_string, _logger);
                    _logger_helper.LogHeader("Transferring study data");
                    ctb.TransferCoreStudyData();
                    _logger_helper.LogHeader("Transferring object data");
                    ctb.TransferCoreObjectData();
                    _logger_helper.LogHeader("Transferring link data");
                    ctb.TransferCoreLinkData();

                    // Include generation of data provenance strings
                    // Need an additional temporary FTW link to mon

                    _logger_helper.LogHeader("Finish");
                    _mon_repo.SetUpTempFTW(_credentials, "mon");
                    ctb.GenerateProvenanceData();
                    _mon_repo.DropTempFTW("mon");
                }


                if (opts.do_statistics)
                {
                    int last_agg_event_id = _mon_repo.GetLastAggEventId();
                    StatisticsBuilder stb = new StatisticsBuilder(last_agg_event_id, _credentials, _mon_repo, _logger);
                    stb.GetStatisticsBySource();
                    stb.GetSummaryStatistics();
                }


                if (opts.create_json)
                {
                    string conn_string = _credentials.GetConnectionString("mdr", false);
                    JSONHelper jh = new JSONHelper(conn_string, _logger);

                    // Create json fields.

                    // if tables are to be left as they are, add false as 
                    // an additional boolean (default = true)
                    // if tables are to have further data appended add an integer
                    // offset that represents the records to skip (default = 0)

                    _logger_helper.LogHeader("Creating JSON study data");
                    jh.CreateJSONStudyData(opts.also_do_files);
                    _logger_helper.LogHeader("Creating JSON object data");
                    jh.CreateJSONObjectData(opts.also_do_files);
                }

                _mon_repo.DropTempContextFTWs();

                _logger_helper.LogHeader("Closing Log");
                return 0;
            }

            catch(Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                _logger_helper.LogHeader("Closing Log");
                return -1;
            }

        }
    }
}
