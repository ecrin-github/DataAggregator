using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class Controller
    {
        int source_id;
		DataLayer repo;

		public Controller(DataLayer _repo, int _source_id)
        {
			repo = _repo;
			source_id = _source_id;

        }

		public int UpdateStudyLinkList()
		{
			// examines the study_reference data in the trial registry databases
			// to try and identify the PubMed data that needs to be downloaded through the API

			repo.SetUpTempLinksBySourceTable();
			repo.SetUpTempLinkCollectorTable();
			CopyHelper helper = new CopyHelper();
			IEnumerable<StudyLink> references;

			// get study reference data from ClinicalTrials.gov
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks("ctg", 100120);
			repo.StoreLinks(helper.links_helper, references);
			repo.TransferLinksToCollectorTable(100120);
				
			// get study reference data from EUCTR
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks("euctr", 100123);
			repo.StoreLinks(helper.links_helper, references);
			repo.TransferLinksToCollectorTable(100123);

			// get study reference data from ISRCTN
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks("isrctn", 100126);
			repo.StoreLinks(helper.links_helper, references);
			repo.TransferLinksToCollectorTable(100126);

            // get study reference data from BioLINCC
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks("biolincc", 100900);
			repo.StoreLinks(helper.links_helper, references);
			repo.TransferLinksToCollectorTable(100900);

			// get study reference data from Yoda
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks("yoda", 100901);
			repo.StoreLinks(helper.links_helper, references);
			repo.TransferLinksToCollectorTable(100901);

			// get study reference data from WHO
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks("who", 0);
			repo.StoreLinks(helper.links_helper, references);
			repo.TransferLinksToCollectorTable(100115);

			// store the contents in the data objects source file as required...
			int total = repo.ObtainTotalOfNewLinks();
			repo.TransferNewLinksToDataTable();

			repo.DropTempLinksBySourceTable();
			repo.DropTempLinkCollectorTable();

			return total;
		}

		public void EstablishStudyIds(int source_id)
		{
			CopyHelper helper = new CopyHelper();
			IEnumerable<StudyIds> study_ids;

			// Get the new study data as a set of study records
			// using the ad database as the source

			// set up a temporary table that holds the ad_id, sd_id, 
			// hash_id for all studies
			// It will need to have the study_id in it 
			
			repo.SetUpTempStudyIdsTable();
			study_ids = repo.FetchStudyIds(source_id);
			repo.StoreStudyIds(helper.study_ids_helper, study_ids);

			// do the check of the temp table ids against the study_study links
			
			repo.CheckStudyLinks();

			// Use sql to load that table once the check is done
			// Use sql to back load the ids into the temporary table
			
			repo.UpdateAllStudyIdsTable(source_id);
		}


		public void EstablishObjectIds(int source_id)
		{
			IEnumerable<ObjectIds> object_ids;
			CopyHelper helper = new CopyHelper();

			repo.SetUpTempObjectIdsTable();
			object_ids = repo.FetchObjectIds(source_id);
			repo.StoreObjectIds(helper.object_ids_helper, object_ids);

			// do the check of the temp table ids against the study_study links
			
			//repo.CheckObjectLinks();

			// Use sql to load that table once the check is done
			// Use sql to back load the ids into the temporary table
			
			//repo.UpdateAllObjectIdsTable(source_id);
		}

		public void LoadStudyData(int source_id)
		{
			repo.LoadStudyData(source_id);
			repo.LoadStudyIdentifiers(source_id);
			repo.LoadStudyTitles(source_id);
			repo.LoadStudyRelationShips(source_id);
			repo.LoadStudyContributors(source_id);
			repo.LoadStudyTopics(source_id);
			repo.LoadStudyFeatures(source_id);
		}

		public void LoadObjectData(int source_id)
		{
			repo.LoadObjectData(source_id); 
			repo.LoadObjectDatasets(source_id);
			repo.LoadObjectInstances(source_id);
			repo.LoadObjectTitles(source_id);
			repo.LoadObjectDates(source_id);
			repo.LoadObjectContributors(source_id);
			repo.LoadObjectTopics(source_id);
			repo.LoadObjectRelationships(source_id);
		}

		public void DropTempTables()
		{
			repo.DropTempStudyIdsTable();
			repo.DropTempObjectIdsTable();
		}

    }
}
