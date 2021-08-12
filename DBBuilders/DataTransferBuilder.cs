using System.Collections.Generic;
using Serilog;

namespace DataAggregator
{
    public class DataTransferBuilder
    {
        ISource _source;
        ILogger _logger;
        string _schema_name;
        string _source_conn_string;

        StudyDataTransferrer st_tr;
        ObjectDataTransferrer ob_tr;

        int source_id;

        public DataTransferBuilder(ISource source, string schema_name, 
            string source_conn_string, string connString, ILogger logger)
        {
            _source = source;
            _logger = logger;
            _schema_name = schema_name;
            _source_conn_string = source_conn_string;

            source_id = _source.id;
            st_tr = new StudyDataTransferrer(connString, _logger);
            ob_tr = new ObjectDataTransferrer(connString, _logger);

        }


        public void ProcessStudyIds()
        {
            // Get the new study data as a set of study records
            // using the ad database as the source.
            // set up a temporary table that holds the sd_sid, 
            // for all studies, and then fill it.

            st_tr.SetUpTempStudyIdsTable();
            IEnumerable<StudyId> study_ids = st_tr.FetchStudyIds(source_id, _source_conn_string);
            _logger.Information("Study Ids obtained");
            st_tr.StoreStudyIds(CopyHelpers.study_ids_helper, study_ids);
            _logger.Information("Study Ids stored");

            // Do the check of the temp table ids against the study_study links.
            // Change the table to reflect the 'preferred' Ids.
            // Back load the correct study ids into the temporary table.

            st_tr.CheckStudyLinks();
            _logger.Information("Study Ids checked");
            st_tr.UpdateAllStudyIdsTable(source_id);
            _logger.Information("Study Ids processed");
        }


        public int TransferStudyData()
        {
            int study_number = st_tr.LoadStudies(_schema_name);
            st_tr.LoadStudyIdentifiers(_schema_name);
            st_tr.LoadStudyTitles(_schema_name);
            if (_source.has_study_contributors) st_tr.LoadStudyContributors(_schema_name);
            if (_source.has_study_topics) st_tr.LoadStudyTopics(_schema_name);
            if (_source.has_study_features) st_tr.LoadStudyFeatures(_schema_name);
            if (_source.has_study_relationships) st_tr.LoadStudyRelationShips(_schema_name);
            st_tr.DropTempStudyIdsTable();
            return study_number;
        }


        public void ProcessStudyObjectIds()
        {
            // Set up temp tables and fill the first with the sd_oids, 
            // parent sd_sids, dates of data fetch, of the objects in 
            // the source database.

            ob_tr.SetUpTempObjectIdsTables();
            IEnumerable<ObjectId> object_ids = ob_tr.FetchObjectIds(source_id, _source_conn_string);
            _logger.Information("Object Ids obtained");
            ob_tr.StoreObjectIds(CopyHelpers.object_ids_helper, object_ids);
            _logger.Information("Object Ids stored");

            // Update the object parent ids against the all_ids_studies table

            ob_tr.UpdateObjectsWithStudyIds(source_id);

            // Carry out a check for (currently very rare) duplicate
            // objects (i.e. that have been imported before with the data 
            // from another source). [TO IMPLEMENT}
            ob_tr.CheckStudyObjectsForDuplicates(source_id);

            // Update the database all objects ids table and derive a 
            // small table that lists the object Ids for all objects
            ob_tr.UpdateAllObjectIdsTable(source_id);
            _logger.Information("Object Ids updated");

            ob_tr.FillObjectsToAddTable(source_id);
            _logger.Information("Object Ids processed");
        }


        public void ProcessStandaloneObjectIds(IEnumerable<Source> sources)
        {
            ob_tr.SetUpTempObjectIdsTables();

            // process the data using available object-study links
            // (may be multiple study links per object)
            // exact process likely to differ with different standalone
            // object sources - at the moment only PubMed in this category

            if (source_id == 100135)
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
                _logger.Information("PMID bank object Ids obtained");

                // Loop threough the study databases that hold
                // study_reference tables, i.e. with pmid ids
                foreach (Source s in sources)
                {
                    if (s.has_study_references)
                    {
                        IEnumerable<PMIDLink> source_references = pm_tr.FetchSourceReferences(s.id, s.database_name);
                        pm_tr.StorePMIDLinks(CopyHelpers.pmid_links_helper, source_references);
                    }
                }
                _logger.Information("PMID source object Ids obtained");

                pm_tr.FillDistinctPMIDsTable();
                pm_tr.DropTempPMIDTable();

                // Try and tidy some of the worst data anomalies
                // before updating the data to the permanent tables.

                pm_tr.CleanPMIDsdsidData1();
                pm_tr.CleanPMIDsdsidData2();
                pm_tr.CleanPMIDsdsidData3();
                pm_tr.CleanPMIDsdsidData4();
                _logger.Information("PMID Ids cleaned");

                // Transfer data to all_ids_data_objects table.

                pm_tr.TransferPMIDLinksToObjectIds();
                ob_tr.UpdateObjectsWithStudyIds(source_id);
                _logger.Information("Object Ids matched to study ids");

                // Use study-study link table to get preferred sd_sid
                // then drop any resulting duplicates from study-pmid table
                pm_tr.InputPreferredSDSIDS();

                // add in study-pmid links to all_ids_objects
                ob_tr.UpdateAllObjectIdsTable(source_id);
                _logger.Information("PMID Ids added to table");

                // use min of ids to set all object ids the same for the same pmid
                pm_tr.ResetIdsOfDuplicatedPMIDs();
                _logger.Information("PMID Ids deduplicatedd");

                // make new table of distinct pmids to add                 
                ob_tr.FillObjectsToAddTable(source_id);
                _logger.Information("PMID Ids processed");
            }
        }


        public int TransferObjectData()
        {
            // Add new records where status indicates they are new
            int object_number = ob_tr.LoadDataObjects(_schema_name);
            if (_source.has_object_datasets) ob_tr.LoadObjectDatasets(_schema_name);
            ob_tr.LoadObjectInstances(_schema_name);
            ob_tr.LoadObjectTitles(_schema_name);
            if (_source.has_object_dates) ob_tr.LoadObjectDates(_schema_name);
            if (_source.has_object_rights) ob_tr.LoadObjectRights(_schema_name);
            if (_source.has_object_relationships) ob_tr.LoadObjectRelationships(_schema_name);
            if (_source.has_object_pubmed_set)
            {
                ob_tr.LoadObjectContributors(_schema_name);
                ob_tr.LoadObjectTopics(_schema_name);
                ob_tr.LoadObjectDescriptions(_schema_name);
                ob_tr.LoadObjectIdentifiers(_schema_name);
            }
            ob_tr.DropTempObjectIdsTable();
            return object_number;
        }
    }
}
