using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
	public class StudyLinkBuilder
	{
		DataLayer repo;
		StudyLinksHelper slh;

		public StudyLinkBuilder(DataLayer _repo)
		{
		   repo = _repo;
           slh = new StudyLinksHelper(repo);
	    }
			
	    public void CollectStudyStudyLinks(IEnumerable<DataSource> sources)
		{
			// Loop through it calling the link helper functions
			// sources are called in 'preference order' starting
			// with clinical trials.gov...
			slh.SetUpTempLinksBySourceTable();
			slh.SetUpTempPreferencesTable(sources);
			slh.SetUpTempLinkCollectorTable();
			foreach (DataSource ds in sources)
			{
				// Aggregate the study-study links and store them
				// in the Collector table in the correct arrangement
				// i.e. lower rated sources inthe 'preferred' fields

				slh.TruncateLinksBySourceTable();
				IEnumerable<StudyLink> links = slh.FetchLinks(ds.id, ds.database_name);
				slh.StoreLinksInTempTable(IdCopyHelpers.links_helper, links);
				if (ds.id != 100120) slh.TidyNCTIds();
				if (ds.id != 100126) slh.TidyISRCTNIds();
				slh.TransferLinksToCollectorTable();
			}
		}


		public void ProcessStudyStudyLinks()
		{
			// Create a table with the distinct values obtained 
			// from the aggregation process, then 'cascade' links
			// to ensure that only the most preferred study id is
			// identified in the 'preferred' fields.

			slh.CreateDistinctSourceLinksTable();
			slh.CascadeLinksInDistinctLinksTable();

			// Identify and remove studies that have links to more than 1
			// study in another registry - these form study-study relationships
			// rather than simple 1-to-1 study links

			slh.ManageLinkedPreferredSources();
			slh.ManageLinkedNonPreferredSources();

			// Identify and repair missing cascade steps
			// Then repeat the cascade telescoping process as above

			slh.ManageIncompleteLinks();
			slh.CascadeLinksInDistinctLinksTable();

			// Transfer the (distinct) resultant set into the 
			// maion links table and tidy up
			slh.TransferNewLinksToDataTable();
			slh.DropTempTables();
		}

    }
}
