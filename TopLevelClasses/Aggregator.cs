﻿using System.Collections.Generic;
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
                // N.B. When testing mdr_conn_string points to the test database
                // If not testing it points to the 'core' mdr database

                string dest_conn_string = _credentials.GetConnectionString("mdr", opts.testing);
                _mon_repo.SetUpTempContextFTWs(_credentials, dest_conn_string);

                if (opts.transfer_data)
                {
                    // In the mdr database, establish new tables, 
                    // for the three schemas st, ob, nk (schemas should already exist)

                    _logger_helper.LogHeader("Establishing aggregate schemas");
                    SchemaBuilder sb = new SchemaBuilder(dest_conn_string);
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

                    StudyLinkBuilder slb = new StudyLinkBuilder(_credentials, dest_conn_string, opts.testing);
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
                    if (opts.testing)
                    {
                        // ensure all AD tables are present for use in the test db
                        _test_repo.BuildNewADTables();
                    }

                    foreach (Source source in sources)
                    {
                        string source_conn_string = _credentials.GetConnectionString(source.database_name, opts.testing);
                        source.db_conn = source_conn_string;

                        // in normal non-testing environment schema name is the ad tables in a FTW - i.e. <db name>_ad
                        // also use credentials here to get the connection string for the source database
                        // In testing environment source schema name will simply be 'ad', and the ad table data
                        // all has to be transferred from adcomp before each source transfer...

                        string schema_name = "";
                        if (opts.testing)
                        {
                            schema_name = "ad";
                            _test_repo.TransferADTableData(source);
                        }
                        else
                        {
                            schema_name = _mon_repo.SetUpTempFTW(_credentials, source.database_name, dest_conn_string);
                        }

                        DataTransferBuilder tb = new DataTransferBuilder(source, schema_name, dest_conn_string, _logger);
                        _logger_helper.LogStudyHeader(false, "Transferring data for " + source.database_name);
                        if (source.has_study_tables)
                        {
                            /*
                            _logger_helper.LogHeader("Process study Ids");
                            tb.ProcessStudyIds();
                            _logger_helper.LogHeader("Transfer study data");
                            num_studies_imported += tb.TransferStudyData();
                            _logger_helper.LogHeader("Process object Ids");
                            tb.ProcessStudyObjectIds();
                            */
                        }
                        else
                        {
                             tb.ProcessStandaloneObjectIds(sources, _credentials, opts.testing);  // for now, just PubMed
                        }

                        if (source.id == 100135) // temporary
                        {
                            _logger_helper.LogHeader("Transfer object data");
                            num_objects_imported += tb.TransferObjectData();
                            
                        }

                        _mon_repo.DropTempFTW(source.database_name, dest_conn_string);
                    }

                    // Also use the study groups data to insert additional study_relationship records
                    slb.CreateStudyGroupRecords();

                    // Update aggregation event record.

                    agg_event.num_studies_imported = num_studies_imported;
                    agg_event.num_objects_imported = num_objects_imported;

                    agg_event.num_total_studies = _mon_repo.GetAggregateRecNum("studies", "st", dest_conn_string);
                    agg_event.num_total_objects = _mon_repo.GetAggregateRecNum("data_objects", "ob", dest_conn_string);
                    agg_event.num_total_study_object_links = _mon_repo.GetAggregateRecNum("data_object_identifiers", "nk", dest_conn_string);

                    if (!opts.testing)
                    {
                        _mon_repo.StoreAggregationEvent(agg_event);
                    }
                }


                if (opts.create_core)
                {
                    // create core tables

                    CoreBuilder cb = new CoreBuilder(dest_conn_string, _logger);
                    
                    _logger_helper.LogHeader("Set up");
                    cb.DeleteCoreTables();
                    _logger.Information("Core tables dropped");
                    cb.BuildNewCoreTables();
                    _logger.Information("Core tables created");

                    // temp 
  
                    // transfer data to core tables
                    CoreTransferBuilder ctb = new CoreTransferBuilder(dest_conn_string, _logger);
                    _logger_helper.LogHeader("Transferring study data");
                    ctb.TransferCoreStudyData();
                    _logger_helper.LogHeader("Transferring object data");
                    ctb.TransferCoreObjectData();
                    _logger_helper.LogHeader("Transferring link data");
                    ctb.TransferCoreLinkData();
                    
                    // Generation of data provenance strings
                    // Need an additional temporary FTW link to mon
                    _logger_helper.LogHeader("Generating provenance data");
                    _mon_repo.SetUpTempFTW(_credentials, "mon", dest_conn_string);
                    ctb.GenerateProvenanceData();
                    _mon_repo.DropTempFTW("mon", dest_conn_string);


                    // set up study search data
                    CoreSearchBuilder csb = new CoreSearchBuilder(dest_conn_string, _logger);
                    _logger_helper.LogHeader("Setting up Study Text Search data");
                    csb.CreateStudyFeatureSearchData();
                    csb.CreateStudyObjectSearchData();
                    csb.CreateStudyTextSearchData();
                }


                if (opts.do_statistics)
                {
                    int last_agg_event_id = _mon_repo.GetLastAggEventId();
                    StatisticsBuilder stb = new StatisticsBuilder(last_agg_event_id, _credentials, _mon_repo, _logger, opts.testing);
                    if (!opts.testing)
                    {
                        stb.GetStatisticsBySource();
                    }
                    stb.GetSummaryStatistics();
                }


                if (opts.create_json)
                {
                    string conn_string = _credentials.GetConnectionString("mdr", opts.testing);
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

                _mon_repo.DropTempContextFTWs(dest_conn_string);

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
