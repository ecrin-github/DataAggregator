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
			ag.AggregateData();
            return 0;
			
			/*
			// Check harvest type id is valid. 

			int harvest_type_id = opts.harvest_type_id;
			if (harvest_type_id != 1 && harvest_type_id != 2 && harvest_type_id != 3)
			{
				WriteLine("Sorry - the harvest type argument does not correspond to 1, 2 or 3");
				return -1;
			}
			*/

			
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
		[Option('t', "harvest_type_id", Required = false, HelpText = "Integer representing (possible future) type of aggregation activity" + " (1 = full, 2 =url access dates only etc.).")]
		public int harvest_type_id { get; set; }
	}

}
