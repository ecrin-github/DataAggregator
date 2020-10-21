using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAggregator
{
	public class Aggregator
	{
		int agg_event_id;
		LoggingDataLayer logging_repo;

		public Aggregator()
        {
			logging_repo = new LoggingDataLayer();
			agg_event_id = logging_repo.GetNextAggEventId();
		}

		public void AggregateData(bool do_statistics, bool transfer_data, bool create_core, 
			                      bool create_json, bool also_do_files)
		{
			StringHelpers.SendHeader("Setup");
			StringHelpers.SendFeedback("transfer data =  " + transfer_data);
			StringHelpers.SendFeedback("create core =  " + create_core);
			StringHelpers.SendFeedback("create json =  " + create_json);
			StringHelpers.SendFeedback("also do json files =  " + also_do_files);
			StringHelpers.SendFeedback("do statistics =  " + do_statistics);


			DataLayer repo = new DataLayer("mdr");
			LoggingDataLayer logging_repo = new LoggingDataLayer();
            // set up the context DB as two sets of foreign tables
			// as it is used in several places
			repo.SetUpTempContextFTWs();

			if (do_statistics)
            {
				StatisticsBuilder stb = new StatisticsBuilder(logging_repo, agg_event_id);
				stb.GetStatisticsBySource();
			}

			if (transfer_data)
			{
				// Establish the mdr and logging repo layers.
				StringHelpers.SendHeader("Establish aggregate schemas");

				// In the mdr database, establish new tables, 
				// for the three schemas st, ob, nk (schemas should already exist)
				SchemaBuilder sb = new SchemaBuilder(repo.ConnString);
				sb.DeleteStudyTables();
				sb.DeleteObjectTables();
				sb.DeleteLinkTables();

				sb.BuildNewStudyTables();
				sb.BuildNewObjectTables();
				sb.BuildNewLinkTables();
				StringHelpers.SendFeedback("Tables created");

				// construct the aggregation event record
				AggregationEvent agg_event = new AggregationEvent(agg_event_id);

				// Derive a new table of inter-study relationships -
				// First get a list of all the study sources and
				// ensure it is sorted correctly.

				IEnumerable<Source> sources = logging_repo.RetrieveDataSources()
										   .OrderBy(s => s.preference_rating);
				StringHelpers.SendFeedback("Sources obtained");

				StudyLinkBuilder slb = new StudyLinkBuilder(repo);
				slb.CollectStudyStudyLinks(sources);
				slb.ProcessStudyStudyLinks();
				StringHelpers.SendFeedback("Study-study links identified");

				// Start the data transfer process
				StringHelpers.SendHeader("Data Transfer");

				// Loop through the study sources (in preference order)
				// In each case establish and then drop the source tables 
				// in a foreign table wrapper

				foreach (Source s in sources)
				{
					string schema_name = repo.SetUpTempFTW(s.database_name);
					string conn_string = logging_repo.FetchConnString(s.database_name);
					DataTransferBuilder tb = new DataTransferBuilder(s, schema_name, conn_string, logging_repo);
					if (s.has_study_tables)
					{
						tb.ProcessStudyIds();
						tb.TransferStudyData();
						tb.ProcessStudyObjectIds();
					}
					else
                    {
						tb.ProcessStandaloneObjectIds();
					}
					tb.TransferObjectData();
					repo.DropTempFTW(s.database_name);
				}

				// Also use the study groups to set up study_relationship records
				slb.CreateStudyGroupRecords();
			}
			

			if (do_statistics)
			{
				StatisticsBuilder stb = new StatisticsBuilder(logging_repo, agg_event_id);
				stb.GetSummaryStatistics();
			}


			if (create_core)
			{
				// create core tables
				SchemaBuilder sb = new SchemaBuilder(repo.ConnString);
				sb.DeleteCoreTables();
				sb.BuildNewCoreTables();

				// transfer data to core tables
				DataTransferBuilder tb = new DataTransferBuilder();
				tb.TransferCoreStudyData();
				tb.TransferCoreObjectData();
				tb.TransferCoreLinkData();

				// Include generation of data provenance strings
				// Need an additional temporary FTW link to mon
				repo.SetUpTempFTW("mon");
				tb.GenerateProvenanceData();
				repo.DropTempFTW("mon");
			}


			if (create_json)
			{
				string conn_string = logging_repo.FetchConnString("mdr");
				JSONHelper jh= new JSONHelper(conn_string);
				jh.CreateJSONTables();

				// Create json fields.
				jh.CreateJSONStudyData(also_do_files);
				jh.CreateJSONObjectData(also_do_files);
			}

			repo.DropTempContextFTWs();
		}
	}
}
