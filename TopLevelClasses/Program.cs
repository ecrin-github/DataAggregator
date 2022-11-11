using System;
using System.IO;
using System.Reflection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataAggregator
{
    class Program
    {
        static void Main(string[] args)
        {
            // Set up file based configuration environment.

            var configFiles = new ConfigurationBuilder()
                .SetBasePath(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location))
                .AddJsonFile("appsettings.json")
                .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", true)
                .Build();

            // Set up the host for the app,
            // adding the services used in the system to support DI
            // and including Serilog

            IHost host = Host.CreateDefaultBuilder()
                 .UseContentRoot(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location))
                 .ConfigureAppConfiguration(builder =>
                 {
                     builder.AddConfiguration(configFiles);
                 })
                 .ConfigureServices((hostContext, services) =>
                 {
                     // Register services (or develop a comp root)
                     services.AddSingleton<ICredentials, Credentials>();
                     services.AddSingleton<IParametersChecker, ParametersChecker>();
                     services.AddSingleton<IAggregator, Aggregator>();
                     services.AddSingleton<IMonitorDataLayer, MonitorDataLayer>();
                     services.AddSingleton<ITestingDataLayer, TestingDataLayer>();
                     services.AddTransient<ISource, Source>();
                 })
                 .Build();

            // Check the command line arguments to ensure they are valid,
            // If they are, start the program by instantiating the Harvester object
            // and telling it to run. The parameter checking process also creates
            // a singleton monitor repository.

            Credentials creds = ActivatorUtilities.CreateInstance<Credentials>(host.Services);
            ParametersChecker paramChecker = ActivatorUtilities.CreateInstance<ParametersChecker>(host.Services);
            Options opts = paramChecker.ObtainParsedArguments(args);

            if (opts != null && paramChecker.ValidArgumentValues(opts))
            {
                string logFilePath = paramChecker.LoggingfFilePath;
                Aggregator aggregator = new Aggregator(logFilePath, creds);
                Environment.ExitCode = aggregator.AggregateData(opts);
            }


        }
    }

}
