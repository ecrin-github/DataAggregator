using CommandLine;
using Serilog;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    internal class ParametersChecker : IParametersChecker
    {
        private ILogger _logger;
        private ILoggerHelper _logger_helper;

        public ParametersChecker(ILogger logger, ILoggerHelper logger_helper)
        {
            _logger = logger;
            _logger_helper = logger_helper;
        }

        // Parse command line arguments and return true only if no errors.
        // Otherwise log errors and return false.

        public Options ObtainParsedArguments(string[] args)
        {
            var parsedArguments = Parser.Default.ParseArguments<Options>(args);
            if (parsedArguments.Tag.ToString() == "NotParsed")
            {
                HandleParseError(((NotParsed<Options>)parsedArguments).Errors);
                return null;
            }
            else
            {
                return ((Parsed<Options>)parsedArguments).Value;
            }
        }

        // Parse command line arguments and return true if values are valid.
        // Otherwise log errors and return false.

        public bool ValidArgumentValues(Options opts)
        {
            try
            {
                // Need at least one of D, C, J or S to be true
                // F only valid if J is present (but F option being dropped)

                // Need to add in a test option

                if ((opts.transfer_data == false)
                    && (opts.create_core == false)
                    && (opts.create_json == false)
                    && (opts.do_statistics == false))
                {
                    throw new Exception("None of the allowed parameters appear to be present!");
                }

                return true;    // OK the program can run!

            }

            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                _logger.Information("Harvester application aborted");
                _logger_helper.LogHeader("Closing Log");
                return false;
            }

        }


        private void HandleParseError(IEnumerable<Error> errs)
        {
            // log the errors
            _logger.Error("Error in the command line arguments - they could not be parsed");
            int n = 0;
            foreach (Error e in errs)
            {
                n++;
                _logger.Error("Error {n}: Tag was {Tag}", n.ToString(), e.Tag.ToString());
                if (e.GetType().Name == "UnknownOptionError")
                {
                    _logger.Error("Error {n}: Unknown option was {UnknownOption}", n.ToString(), ((UnknownOptionError)e).Token);
                }
                if (e.GetType().Name == "MissingRequiredOptionError")
                {
                    _logger.Error("Error {n}: Missing option was {MissingOption}", n.ToString(), ((MissingRequiredOptionError)e).NameInfo.NameText);
                }
                if (e.GetType().Name == "BadFormatConversionError")
                {
                    _logger.Error("Error {n}: Wrongly formatted option was {MissingOption}", n.ToString(), ((BadFormatConversionError)e).NameInfo.NameText);
                }
            }
            _logger.Information("Harvester application aborted");
            _logger_helper.LogHeader("Closing Log");
        }

    }


    public class Options
    {
        // Lists the command line arguments and options
        [Option('D', "transfer and aggregate data", Required = false, HelpText = "Indicates that data should be imported from source systems and aggregate st, ob, nk tables constructed ")]
        public bool transfer_data { get; set; }

        [Option('C', "create core table data", Required = false, HelpText = "Indicates that the core tables should be crated and filled from the aggregate tables.")]
        public bool create_core { get; set; }

        [Option('J', "create json", Required = false, HelpText = "Indicates json fields should be constructed from the core table data.")]
        public bool create_json { get; set; }

        [Option('F', "create json files", Required = false, HelpText = "Indicates json files should also be constructed from the core table data. Only has an effect if -J parameter present ")]
        public bool also_do_files { get; set; }

        [Option('S', "do statistics", Required = false, HelpText = "Summarises record numbers, of each sort, in different sources and in the summary and core tables")]
        public bool do_statistics { get; set; }
    }

}
      
