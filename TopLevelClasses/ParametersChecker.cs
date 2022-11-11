using CommandLine;
using System;
using System.Collections.Generic;


namespace DataAggregator
{
    internal class ParametersChecker : IParametersChecker
    {
        private LoggingHelper _logging_helper;

        public ParametersChecker()
        {
            _logging_helper = new LoggingHelper();
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

        public string LoggingfFilePath => _logging_helper.LogFilePath;
       
        // Parse command line arguments and return true if values are valid.
        // Otherwise log errors and return false.

        public bool ValidArgumentValues(Options opts)
        {
            _logging_helper.LogHeader("Checking Parameters");

            try
            {
                if (opts.testing)
                {
                    // no particular requirement here
                    // can drop straight through to run the program
                    // but set the other parameters as true so all functions are tested

                    opts.transfer_data = true;
                    opts.create_core = true;
                    opts.create_json = true;
                    opts.do_statistics = true;

                }
                else if ((opts.transfer_data == false)
                    && (opts.create_core == false)
                    && (opts.create_json == false)
                    && (opts.do_statistics == false))
                {
                    // If not testing need at least one of D, C, J or S to be true

                    throw new Exception("None of the allowed optional parameters appear to be present!");
                }
                else if (opts.also_do_files)
                {
                    // F only valid if J is present (but F option being dropped)

                    if(opts.create_json == false)
                    {
                        throw new Exception("F parameter can only be provided if J paramewter also provided");
                    }
                }

                _logging_helper.SwitchLog();
                return true;    // OK the program can run!
            }

            catch (Exception e)
            {
                _logging_helper.LogHeader("INVALID PARAMETERS");
                _logging_helper.LogParameters(opts);
                _logging_helper.LogCodeError("Aggregation application aborted", e.Message, e.StackTrace);
                _logging_helper.CloseLog();
                return false;
            }

        }


        private void HandleParseError(IEnumerable<Error> errs)
        {
            // log the errors
            _logging_helper.LogHeader("UNABLE TO PARSE PARAMETERS");
            _logging_helper.LogHeader("Error in input parameters");
            _logging_helper.LogLine("Error in the command line arguments - they could not be parsed");

            int n = 0;
            foreach (Error e in errs)
            {
                n++;
                _logging_helper.LogParseError("Error {n}: Tag was {Tag}", n.ToString(), e.Tag.ToString());
                if (e.GetType().Name == "UnknownOptionError")
                {
                    _logging_helper.LogParseError("Error {n}: Unknown option was {UnknownOption}", n.ToString(), ((UnknownOptionError)e).Token);
                }
                if (e.GetType().Name == "MissingRequiredOptionError")
                {
                    _logging_helper.LogParseError("Error {n}: Missing option was {MissingOption}", n.ToString(), ((MissingRequiredOptionError)e).NameInfo.NameText);
                }
                if (e.GetType().Name == "BadFormatConversionError")
                {
                    _logging_helper.LogParseError("Error {n}: Wrongly formatted option was {MissingOption}", n.ToString(), ((BadFormatConversionError)e).NameInfo.NameText);
                }
            }
            _logging_helper.LogLine("Aggregation application aborted");
            _logging_helper.CloseLog();
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

        [Option('T', "use test data", Required = false, HelpText = "Carry out D, C, S and J but usiung test data only, in the test database")]
        public bool testing { get; set; }
    }

}
      
