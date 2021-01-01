using CommandLine;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DataAggregator
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var parsedArguments = Parser.Default.ParseArguments<Options>(args);
            await parsedArguments.WithParsedAsync(opts => RunOptionsAndReturnExitCodeAsync(opts));
            await parsedArguments.WithNotParsedAsync((errs) => HandleParseErrorAsync(errs));
        }

        static async Task<int> RunOptionsAndReturnExitCodeAsync(Options opts)
        {
            // N.B. The aggregation process re-aggregates all the data from scratch.

            LoggingDataLayer logging_repo = new LoggingDataLayer();
            Aggregator ag = new Aggregator(logging_repo);
            logging_repo.OpenLogFile(opts);

            try
            {
                await ag.AggregateDataAsync(opts);
                return 0;
            }
            catch (Exception e)
            {
                logging_repo.LogError("Unhandled exception: " + e.Message);
                logging_repo.LogLine(e.StackTrace);
                logging_repo.LogLine(e.TargetSite.Name);
                logging_repo.CloseLog();
                return -1;
            }
        }

        static Task HandleParseErrorAsync(IEnumerable<Error> errs)
        {
            // try and log error and details


            return Task.CompletedTask;
        }
    }


    public class Options
    {
        // Lists the command line arguments and options
        [Option('D', "transfer data", Required = false, HelpText = "Indicates thast data should be imported from source systems and aggregate st, ob, nk tables constructed ")]
        public bool transfer_data { get; set; }
        
        [Option('C', "harvest_type_id", Required = false, HelpText = "Indicates that the core tables should be crated and filled from the aggregate tables.")]
        public bool create_core { get; set; }

        [Option('J', "create json", Required = false, HelpText = "Indicates json fields should be constructed from the core table data.")]
        public bool create_json { get; set; }

        [Option('F', "create json files", Required = false, HelpText = "Indicates json files should also be constructed from the core table data. Only has an effect if -J parameter present ")]
        public bool also_do_files { get; set; }

        [Option('S', "do statistics", Required = false, HelpText = "Summarises record numbers, of each sort, in different sources and in the summary and core tables")]
        public bool do_statistics { get; set; }
    }

}
