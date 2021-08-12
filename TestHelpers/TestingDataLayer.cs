using Dapper;
using Npgsql;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class TestingDataLayer : ITestingDataLayer
    {
        ICredentials _credentials;
        NpgsqlConnectionStringBuilder builder;
        private string _db_conn;
        ILogger _logger;

        /// <summary>
        /// Constructor is used to build the connection string, 
        /// using a credentials object that has the relevant credentials 
        /// from the app settings, themselves derived from a json file.
        /// </summary>
        /// 
        public TestingDataLayer(ILogger logger, ICredentials credentials)
        {
            builder = new NpgsqlConnectionStringBuilder();

            builder.Host = credentials.Host;
            builder.Username = credentials.Username;
            builder.Password = credentials.Password;

            builder.Database = "test";
            _db_conn = builder.ConnectionString;

            _credentials = credentials;

            _logger = logger;
        }

        public Credentials Credentials => (Credentials)_credentials;


        public int EstablishExpectedData()
        {
            try
            {
                _logger.Information("STARTING EXPECTED DATA ASSEMBLY");
                /*
                TestSchemaBuilder tsb = new TestSchemaBuilder(_db_conn);

                tsb.SetUpMonSchema();
                _logger.Information("mon_sf link established");

                tsb.SetUpExpectedTables();
                _logger.Information("Expected Data tables recreated");

                tsb.SetUpSDCompositeTables();
                _logger.Information("SD composite test data tables recreated");

                ExpectedDataBuilder edb = new ExpectedDataBuilder(_db_conn);

                edb.InitialiseTestStudiesList();
                _logger.Information("List of test studies inserted");

                edb.LoadInitialInputTables();
                _logger.Information("Data loaded from manual inspections");

                edb.CalculateAndAddOIDs();
                _logger.Information("OIDs calculated and inserted");

                tsb.TearDownForeignSchema();
                _logger.Information("mon_sf link deleted");
                */
                return 0;
            }

            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                _logger.Information("Closing Log");
                return -1;
            }
        }


        public void TransferTestSDData(ISource source)
        {
            /*
            TransferSDDataBuilder tdb = new TransferSDDataBuilder(source);
            tdb.DeleteExistingStudyData();
            tdb.DeleteExistingObjectData();
            _logger.Information("Any existing SD test data for source " + source.id + " removed from CompSD");

            tdb.TransferStudyData();
            tdb.TransferObjectData();
            _logger.Information("New SD test data for source " + source.id + " added to CompSD");
        */
       }


        public IEnumerable<int> ObtainTestSourceIDs()
        {
            string sql_string = @"select distinct source_id 
                                 from expected.source_studies;";

            using (var conn = new NpgsqlConnection(_db_conn))
            {
                return conn.Query<int>(sql_string);
            }
        }

    }
}
