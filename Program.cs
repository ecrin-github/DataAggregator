using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static System.Console;

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
			// For now the aggregation process runs without parameters
			// and re-aggregates all the data from scratch.
			
			Aggregator ag = new Aggregator();
			ag.AggregateData(opts.do_statistics, opts.transfer_data, opts.create_core, opts.create_json);
            return 0;
		
		}

		static Task HandleParseErrorAsync(IEnumerable<Error> errs)
		{
			// do nothing for the moment
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

		[Option('J', "create json", Required = false, HelpText = "Indicates json fields and files should be constructed from the core table data.")]
		public bool create_json { get; set; }

		[Option('S', "do statistics", Required = false, HelpText = "Summarises record numbbers, of each sort, in different sources and in the summary and core tables")]
		public bool do_statistics { get; set; }

	}

}
