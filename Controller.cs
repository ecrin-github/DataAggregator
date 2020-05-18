using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace DataAggregator
{
    public class Controller
    {
        int source_id;
		DataLayer repo;
		string mdr_connString;
		StudyDataTransferrer study_trans;
		ObjectDataTransferrer object_trans;

		public Controller(DataLayer _repo, int _source_id)
        {
			repo = _repo;
			source_id = _source_id;
			mdr_connString = repo.GetMDRConnString();
			study_trans = new StudyDataTransferrer(repo);
			object_trans = new ObjectDataTransferrer(repo);
		}

		public int UpdateStudyLinkList()
		{
			// examines the study_reference data in the trial registry databases
			// to try and identify the PubMed data that needs to be downloaded through the API
			StudyLinksGenerator links = new StudyLinksGenerator(repo);

			links.SetUpTempLinksBySourceTable();
			links.SetUpTempLinkCollectorTable();
			IEnumerable<StudyLink> references;

			// get study reference data from ClinicalTrials.gov
			links.TruncateLinksBySourceTable();
			references = links.FetchLinks(100120);
			links.StoreLinks(IdCopyHelpers.links_helper, references);
			links.TransferLinksToCollectorTable();

			// get study reference data from EUCTR
			links.TruncateLinksBySourceTable();
			references = links.FetchLinks(100123);
			links.StoreLinks(IdCopyHelpers.links_helper, references);
			links.TransferLinksToCollectorTable();

			// get study reference data from ISRCTN
			links.TruncateLinksBySourceTable();
			references = links.FetchLinks(100126);
			links.StoreLinks(IdCopyHelpers.links_helper, references);
			links.TransferLinksToCollectorTable();

			// get study reference data from BioLINCC
			links.TruncateLinksBySourceTable();
			references = links.FetchLinks(100900);
			links.StoreLinks(IdCopyHelpers.links_helper, references);
			links.TransferLinksToCollectorTable();

			// get study reference data from Yoda
			links.TruncateLinksBySourceTable();
			references = links.FetchLinks(100901);
			links.StoreLinks(IdCopyHelpers.links_helper, references);
			links.TransferLinksToCollectorTable();

			// get study reference data from WHO
			links.TruncateLinksBySourceTable();
			references = links.FetchLinks(100115);
			links.StoreLinks(IdCopyHelpers.links_helper, references);
			links.TransferLinksToCollectorTable();

			// cascade preferred other study ids
			links.MakeLinksDistinct();
			links.CascadeLinksTable();

			links.FindMultipleRelationships();

			// store the contents in the data objects source file as required...
			int total = links.ObtainTotalOfNewLinks();
			links.TransferNewLinksToDataTable();

			links.DropTempTables();

			return total;
		}
		
		
		public void SetUpTempSchema(string db_name)
		{
			study_trans.SetUpTempFTW(db_name);
		}

		public void EstablishStudyIds(int source_id)
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


		public void EstablishObjectIds(int source_id)
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


		public void LoadStudyData(string schema_name)
		{
			// Add new records where status indicates they are new
			study_trans.LoadStudyData(schema_name);
			study_trans.LoadStudyIdentifiers(schema_name);
			study_trans.LoadStudyTitles(schema_name);
			study_trans.LoadStudyRelationShips(schema_name);
			study_trans.LoadStudyContributors(schema_name);
			study_trans.LoadStudyTopics(schema_name);
			study_trans.LoadStudyFeatures(schema_name);

			// Update records where status indicates they have changed


			// Update date of data fetch if that is all that has changed


		}

		public void LoadObjectData(string schema_name)
		{
			// Add new records where status indicates they are new
			object_trans.LoadObjectData(schema_name);
			object_trans.LoadObjectDatasets(schema_name);
			object_trans.LoadObjectInstances(schema_name);
			object_trans.LoadObjectTitles(schema_name);
			object_trans.LoadObjectDates(schema_name);
			object_trans.LoadObjectContributors(schema_name);
			object_trans.LoadObjectTopics(schema_name);
			object_trans.LoadObjectRelationships(schema_name);

			// Update records where status indicates they have changed




			// Update date of data fetch if that is all that has changed


		}

		public void DropTempTables()
		{
			study_trans.DropTempStudyIdsTable();
			object_trans.DropTempObjectIdsTable();
		}

		public void DropTempSchema(string db_name)
		{
			study_trans.DropTempFTW(db_name);
		}


	}
}
