using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAggregator
{
	class Aggregator
	{
		public void AggregateData(bool transfer_data, bool create_core, bool create_json)
		{
			StringHelpers.SendHeader("Setup");
			StringHelpers.SendFeedback("transfer data =  " + transfer_data);
			StringHelpers.SendFeedback("create core =  " + create_core);
			StringHelpers.SendFeedback("create json =  " + create_json);

			if (transfer_data)
			{
				// Establish the mdr and logging repo layers.

				StringHelpers.SendHeader("Establish aggregate schemas");
				DataLayer repo = new DataLayer("mdr");
				LoggingDataLayer logging_repo = new LoggingDataLayer();

				// In the mdr database, establish new tables, 
				// for the three schemas st, ob, nk (schemas should already exist)

				SchemaBuilder sb = new SchemaBuilder(repo.ConnString);
				sb.DeleteStudyTables();
				sb.DeleteObjectTables();
				sb.DeleteLinkTables();

				sb.BuildNewStudyTables();
				sb.BuildNewObjectTables();
				sb.BuildNewLinkTables();
				StringHelpers.SendFeedback("Tables Created");

				// construct the aggregation record
				int agg_id = logging_repo.GetNextAggEventId();
				AggregationEvent harvest = new AggregationEvent();

				// Derive a new table of inter-study relationships -
				// First get a list of all the study sources and
				// ensure it is sorted correctly....

				IEnumerable<DataSource> sources = logging_repo.RetrieveDataSources()
										   .OrderBy(s => s.preference_rating);
				StringHelpers.SendFeedback("Sources listed");

				StudyLinkBuilder slb = new StudyLinkBuilder(repo);
				slb.CollectStudyStudyLinks(sources);
				slb.ProcessStudyStudyLinks();
				StringHelpers.SendFeedback("Study-study links identified");

				// Start the data transfer process
				StringHelpers.SendHeader("Data Transfer");

				// Loop through the study sources (in preference order)
				foreach (DataSource ds in sources)
				{
					StudyTransferBuilder stb = new StudyTransferBuilder(ds.id);
					stb.EstablishForeignTables();
					stb.ProcessStudyIds();
					stb.TransferStudyData();

					ObjectTransferBuilder otb = new ObjectTransferBuilder(ds.id);
					otb.ProcessObjectIds();
					otb.TransferObjectData();
					otb.DropForeignTables();
				}

				// then add Pubmed data objects - ensure its content has been updated
				// In future may need a separate set of non-study sources


			}

			if (create_core)
			{
				// create core tables


				// transfer data to core tables

			}


			if (create_json)
			{
				// Create json fields


				// create json files

			}
		}
	}
}
