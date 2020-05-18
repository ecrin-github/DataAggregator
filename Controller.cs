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
			IEnumerable<StudyLink> references;

			// get study reference data from ClinicalTrials.gov
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks(100120);
			repo.StoreLinks(IdCopyHelpers.links_helper, references);
			repo.TransferLinksToCollectorTable(100120);
				
			// get study reference data from EUCTR
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks(100123);
			repo.StoreLinks(IdCopyHelpers.links_helper, references);
			repo.TransferLinksToCollectorTable(100123);

			// get study reference data from ISRCTN
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks(100126);
			repo.StoreLinks(IdCopyHelpers.links_helper, references);
			repo.TransferLinksToCollectorTable(100126);

            // get study reference data from BioLINCC
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks(100900);
			repo.StoreLinks(IdCopyHelpers.links_helper, references);
			repo.TransferLinksToCollectorTable(100900);

			// get study reference data from Yoda
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks(100901);
			repo.StoreLinks(IdCopyHelpers.links_helper, references);
			repo.TransferLinksToCollectorTable(100901);

			// get study reference data from WHO
			repo.TruncateLinksBySourceTable();
			references = repo.FetchLinks(100115);
			repo.StoreLinks(IdCopyHelpers.links_helper, references);
			repo.TransferLinksToCollectorTable(100115);

			// store the contents in the data objects source file as required...
			int total = repo.ObtainTotalOfNewLinks();
			repo.TransferNewLinksToDataTable();

			repo.DropTempLinksBySourceTable();
			repo.DropTempLinkCollectorTable();

			return total;
		}

		public void EstablishStudyIds(StudyDataTransfer study_trans, int source_id)
		{
			IEnumerable<StudyIds> study_ids;

			// Get the new study data as a set of study records
			// using the ad database as the source

			// set up a temporary table that holds the ad_id, sd_id, 
			// hash_id for all studies
			// It will need to have the study_id in it 
			study_trans.SetUpTempStudyIdsTable();
			study_ids = study_trans.FetchStudyIds(source_id);
			study_trans.StoreStudyIds(IdCopyHelpers.study_ids_helper, study_ids);

			// do the check of the temp table ids against the study_study links

			study_trans.CheckStudyLinks();

			// Use sql to load that table once the check is done
			// Use sql to back load the ids into the temporary table

			study_trans.UpdateAllStudyIdsTable(source_id);
		}


		public void EstablishObjectIds(ObjectDataTransfer object_trans, int source_id)
		{
			IEnumerable<ObjectIds> object_ids;

			object_trans.SetUpTempObjectIdsTable();
			object_ids = object_trans.FetchObjectIds(source_id);
			object_trans.StoreObjectIds(IdCopyHelpers.object_ids_helper, object_ids);

			// do the check of the temp table ids against the study_study links
			
			//repo.CheckObjectLinks();

			// Use sql to load that table once the check is done
			// Use sql to back load the ids into the temporary table
			
			//repo.UpdateAllObjectIdsTable(source_id);
		}

		public void LoadStudyData(StudyDataTransfer study_trans, int source_id)
		{
			study_trans.LoadStudyData(source_id);
			study_trans.LoadStudyIdentifiers(source_id);
			study_trans.LoadStudyTitles(source_id);
			study_trans.LoadStudyRelationShips(source_id);
			study_trans.LoadStudyContributors(source_id);
			study_trans.LoadStudyTopics(source_id);
			study_trans.LoadStudyFeatures(source_id);
		}

		public void LoadObjectData(ObjectDataTransfer object_trans, int source_id)
		{
			object_trans.LoadObjectData(source_id);
			object_trans.LoadObjectDatasets(source_id);
			object_trans.LoadObjectInstances(source_id);
			object_trans.LoadObjectTitles(source_id);
			object_trans.LoadObjectDates(source_id);
			object_trans.LoadObjectContributors(source_id);
			object_trans.LoadObjectTopics(source_id);
			object_trans.LoadObjectRelationships(source_id);
		}

		public void DropTempTables(StudyDataTransfer study_trans, ObjectDataTransfer object_trans)
		{
			study_trans.DropTempStudyIdsTable();
			object_trans.DropTempObjectIdsTable();
		}

    }
}
