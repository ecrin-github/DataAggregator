using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class DataTransferBuilder
    {
        Source source;
        string schema_name;
        string source_conn_string;
        StudyDataTransferrer st_tr;
        ObjectDataTransferrer ob_tr;
        CoreDataTransferrer core_tr;
        LoggingDataLayer logging_repo;


        public DataTransferBuilder(Source _source, string _schema_name, string _source_conn_string, LoggingDataLayer _logging_repo)
        {
            source = _source;
            DataLayer repo = new DataLayer("mdr");
            st_tr = new StudyDataTransferrer(repo);
            ob_tr = new ObjectDataTransferrer(repo);
            schema_name = _schema_name;
            source_conn_string = _source_conn_string;
            logging_repo = _logging_repo;
        }

        public DataTransferBuilder()
        {
            DataLayer repo = new DataLayer("mdr");
            core_tr = new CoreDataTransferrer(repo);
        }


        public void ProcessStudyIds()
        {
            // Get the new study data as a set of study records
            // using the ad database as the source.
            // set up a temporary table that holds the sd_sid, 
            // for all studies, and then fill it.

            st_tr.SetUpTempStudyIdsTable();
            IEnumerable<StudyId> study_ids = st_tr.FetchStudyIds(source.id, source_conn_string);
            StringHelpers.SendFeedback("Study Ids obtained");
            st_tr.StoreStudyIds(CopyHelpers.study_ids_helper, study_ids);
            StringHelpers.SendFeedback("Study Ids stored");

            // Do the check of the temp table ids against the study_study links.
            // Change the table to reflect the 'preferred' Ids.
            // Back load the correct study ids into the temporary table.

            st_tr.CheckStudyLinks();
            StringHelpers.SendFeedback("Study Ids checked");
            st_tr.UpdateAllStudyIdsTable(source.id);
            StringHelpers.SendFeedback("Study Ids processed");
        }


        public void TransferStudyData()
        {
            st_tr.LoadStudies(schema_name);
            st_tr.LoadStudyIdentifiers(schema_name);
            st_tr.LoadStudyTitles(schema_name);
            if (source.has_study_contributors) st_tr.LoadStudyContributors(schema_name);
            if (source.has_study_topics) st_tr.LoadStudyTopics(schema_name);
            if (source.has_study_features) st_tr.LoadStudyFeatures(schema_name);
            if (source.has_study_relationships) st_tr.LoadStudyRelationShips(schema_name);
            st_tr.DropTempStudyIdsTable();
        }


        public void ProcessStudyObjectIds()
        {
            // Set up temp tables and fill the first with the sd_oids, 
            // parent sd_sids, dates of data fetch, of the objects in 
            // the source database.

            ob_tr.SetUpTempObjectIdsTables();
            IEnumerable<ObjectId> object_ids = ob_tr.FetchObjectIds(source.id);
            StringHelpers.SendFeedback("Object Ids obtained");
            ob_tr.StoreObjectIds(CopyHelpers.object_ids_helper, object_ids);
            StringHelpers.SendFeedback("Object Ids stored");

            // Update the object parent ids against the all_ids_studies table

            ob_tr.UpdateObjectsWithStudyIds(source.id);

            // Carry out a check for (currently very rare) duplicate
            // objects (i.e. that have been imported before with the data 
            // from another source). [TO IMPLEMENT}
            ob_tr.CheckStudyObjectsForDuplicates(source.id);

            // Update the database all objects ids table and derive a 
            // small table that lists the object Ids for all objects
            ob_tr.UpdateAllObjectIdsTable(source.id);
            StringHelpers.SendFeedback("Object Ids updated");

            ob_tr.FillObjectsToAddTable(source.id);
            StringHelpers.SendFeedback("Object Ids processed");

        }


        public void ProcessStandaloneObjectIds()
        {
            ob_tr.SetUpTempObjectIdsTables();

            // process the data using available object-study links
            // (may be multiple study links per object)
            // exact process likely to differ with different standalone
            // object sources - at the moment only PubMed in this category

            if (source.id == 100135)
            {
                // Get the source -study- pmid link data 
                // A table of PMID bank data was created during data download, but this 
                // may have been date limited (probably was) so the total of records 
                // in the ad tables needs to be used.
                // This needs to be combined with the references in those sources 
                // that conbtain study_reference tables

                PubmedTransferHelper pm_tr = new PubmedTransferHelper();
                pm_tr.SetupTempPMIDTable();
                pm_tr.SetupDistinctPMIDTable();
                
                IEnumerable<PMIDLink> bank_object_ids = pm_tr.FetchBankPMIDs();
                pm_tr.StorePMIDLinks(CopyHelpers.pmid_links_helper, bank_object_ids);
                StringHelpers.SendFeedback("PMID bank object Ids obtained");

                // Loop threough the study databases that hold
                // study_reference tables, i.e. with pmid ids
                IEnumerable<Source> sources = logging_repo.RetrieveDataSources();
                foreach (Source s in sources)
                {
                    if (s.has_study_references)
                    {
                        IEnumerable<PMIDLink> source_references = pm_tr.FetchSourceReferences(s.id, s.database_name);
                        pm_tr.StorePMIDLinks(CopyHelpers.pmid_links_helper, source_references);
                    }
                }
                StringHelpers.SendFeedback("PMID source object Ids obtained");

                pm_tr.FillDistinctPMIDsTable();
                pm_tr.DropTempPMIDTable();

                // Try and tidy some of the worst data anomalies
                // before updating the data to the permanent tables.

                pm_tr.CleanPMIDsdsidData1();
                pm_tr.CleanPMIDsdsidData2();
                pm_tr.CleanPMIDsdsidData3();
                pm_tr.CleanPMIDsdsidData4();
                StringHelpers.SendFeedback("PMID Ids cleaned");

                // Transfer data to all_ids_data_objects table.

                pm_tr.TransferPMIDLinksToObjectIds();
                ob_tr.UpdateObjectsWithStudyIds(source.id);
                StringHelpers.SendFeedback("Object Ids matched to study ids");

                // Use study-study link table to get preferred sd_sid
                // then drop any resulting duplicates from study-pmid table
                ob_tr.InputPreferredSDSIDS();

                // add in study-pmid links to all_ids_objects
                ob_tr.UpdateAllObjectIdsTable(source.id);
                StringHelpers.SendFeedback("PMID Ids added to table");

                // use min of ids to set all object ids the same for the same pmid
                ob_tr.ResetIdsOfDuplicatedPMIDs(source.id);
                StringHelpers.SendFeedback("PMID Ids deduplicatedd");

                // make new table of distinct pmids to add                 
                ob_tr.FillObjectsToAddTable(source.id);
                StringHelpers.SendFeedback("PMID Ids processed");

            }
        }


        public void TransferObjectData()
        {
            // Add new records where status indicates they are new
            ob_tr.LoadDataObjects(schema_name);
            if (source.has_object_datasets) ob_tr.LoadObjectDatasets(schema_name);
            ob_tr.LoadObjectInstances(schema_name);
            ob_tr.LoadObjectTitles(schema_name);
            if (source.has_object_dates) ob_tr.LoadObjectDates(schema_name);
            if (source.has_object_rights) ob_tr.LoadObjectRights(schema_name);
            if (source.has_object_relationships) ob_tr.LoadObjectRelationships(schema_name);
            if (source.has_object_pubmed_set)
            {
                ob_tr.LoadObjectContributors(schema_name);
                ob_tr.LoadObjectTopics(schema_name);
                ob_tr.LoadObjectDescriptions(schema_name);
                ob_tr.LoadObjectIdentifiers(schema_name);
            }
            ob_tr.DropTempObjectIdsTable();
        }


        public void TransferCoreStudyData()
        {
            core_tr.LoadCoreStudyData();
            core_tr.LoadCoreStudyIdentifiers();
            core_tr.LoadCoreStudyTitles();
            core_tr.LoadCoreStudyContributors();
            core_tr.LoadCoreStudyTopics();
            core_tr.LoadCoreStudyFeatures();
            core_tr.LoadCoreStudyRelationShips();
        }


        public void TransferCoreObjectData()
        {
            core_tr.LoadCoreDataObjects();
            core_tr.LoadCoreObjectDatasets();
            core_tr.LoadCoreObjectInstances();
            core_tr.LoadCoreObjectTitles();
            core_tr.LoadCoreObjectDates();
            core_tr.LoadCoreObjectContributors();
            core_tr.LoadCoreObjectTopics();
            core_tr.LoadCoreObjectDescriptions();
            core_tr.LoadCoreObjectIdentifiers();
            core_tr.LoadCoreObjectRelationships();
            core_tr.LoadCoreObjectRights();
        }

        public void TransferCoreLinkData()
        {
            core_tr.LoadStudyObjectLinks();
        }


        public void GenerateProvenanceData()
        {
            core_tr.GenerateStudyProvenanceData();
            core_tr.GenerateObjectProvenanceData();
        }

    }
}
