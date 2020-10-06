using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DataAggregator
{
    class Aggregator
    {
        public void AggregateData()
        {
			// Check each source id is valid and run the program if it is... 
			// Identify source type and location, destination folder
			StringHelpers.SendHeader("Setup");

			// Get the mdr and logging repo layers established
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

			// construct the aggregation record
			int agg_id = logging_repo.GetNextAggEventId();
            AggregationEvent harvest = new AggregationEvent();

			// Derive a new table of inter-study relationships


			// Start the data trransfer process

			// Always begin - for now - with clinical trials.gov

			// then loop through the other study sources (in preference order)

			// then add Pubmed - ensure its content has been updated

		}

    }
}
