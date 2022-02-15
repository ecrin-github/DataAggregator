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
        string _dest_conn_string;

        StudyDataTransferrer st_tr;
        ObjectDataTransferrer ob_tr;

        int source_id;

        public DataTransferBuilder(ISource source, string schema_name, string dest_conn_string, ILogger logger)
        {
            _source = source;
            _logger = logger;
            _schema_name = schema_name;
            _source_conn_string = source.db_conn;
            _dest_conn_string = dest_conn_string;

            source_id = _source.id;
            st_tr = new StudyDataTransferrer(_dest_conn_string, _logger);
            ob_tr = new ObjectDataTransferrer(_dest_conn_string, _logger);

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

            // Match existing studiesm, then
            // Do the check of the temp table ids against the study_study links.
            // Change the table to reflect the 'preferred' Ids.
            // Back load the correct study ids into the temporary table.

            st_tr.MatchExistingStudyIds();
            st_tr.IdentifyNewLinkedStudyIds();
            st_tr.AddNewStudyIds(source_id);
            _logger.Information("Study Ids checked");
            st_tr.CreateTempStudyIdTables(source_id);
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
            ob_tr.MatchExistingObjectIds(source_id);
            ob_tr.UpdateNewObjectsWithStudyIds(source_id);
            ob_tr.AddNewObjectsToIdentifiersTable(source_id);

            // Carry out a check for (currently very rare) duplicate
            // objects (i.e. that have been imported before with the data 
            // from another source). [TO IMPLEMENT}
            ob_tr.CheckNewObjectsForDuplicateTitles(source_id);
            ob_tr.CheckNewObjectsForDuplicateURLs(source_id, _schema_name);
            ob_tr.CompleteNewObjectsStatuses(source_id);
            _logger.Information("Object Ids updated");

            // Update the database all objects iidentifiers table and derive a 
            // small table that lists the object Ids for all objects, and one
            // that lists the ids of possible duplicate objects, to check

            ob_tr.FillObjectsToAddTables(source_id);
            _logger.Information("Object Ids processed");
        }


        public void ProcessStandaloneObjectIds(IEnumerable<Source> sources, ICredentials credentials, bool testing)
        {
            ob_tr.SetUpTempObjectIdsTables();

            // process the data using available object-study links
            // (may be multiple study links per object)
            // exact process likely to differ with different standalone
            // object sources - at the moment only PubMed in this category

            if (source_id == 100135)
            {
                // set up the necessary objects and tables to hold the link data

                PubmedTransferHelper pm_tr = new PubmedTransferHelper(_schema_name, _dest_conn_string, _logger);
                pm_tr.SetupTempPMIDTables();

                // Get the source -study- pmid link data 
                // A table of PMID bank data was created during the last data download, but this 
                // was probably date limited so the total of pubmed 'bank' records needs to be used.
                               
                IEnumerable<PMIDLink> bank_object_ids = pm_tr.FetchBankPMIDs();
                ulong res = pm_tr.StorePMIDLinks(CopyHelpers.pmid_links_helper, bank_object_ids);
                _logger.Information(res.ToString() + " study Ids obtained from PMID 'bank' data");

                // study ids referenced in PubMed data often poorly formed and need cleaning

                pm_tr.CleanPMIDsdsidData();
                _logger.Information("Study Ids in 'Bank' PMID records cleaned");

                // This needs to be combined with the references in those sources 
                // that contain study_reference tables - loop thropugh these...

                res = 0;
                foreach (Source source in sources)
                {
                   
                    if (source.has_study_references)
                    {
                        string source_conn_string = credentials.GetConnectionString(source.database_name, testing);
                        if (testing)
                        {
                            pm_tr.TransferReferencesData(source.id);
                        }
                        IEnumerable<PMIDLink> source_references = pm_tr.FetchSourceReferences(source.id, source_conn_string);
                        res += pm_tr.StorePMIDLinks(CopyHelpers.pmid_links_helper, source_references);
                    }
                }
                _logger.Information(res.ToString() + " PMID records obtained from registry linked reference records");


                // Transfer data to 'standard' data_object_identifiers table.
                // and insert the 'correct' study_ids against the sd_sid
                // (all are known as studies already added)

                pm_tr.TransferPMIDLinksToTempObjectIds();
                pm_tr.UpdateTempObjectIdsWithStudyDetails();  

                // Duplication of PMIDs is from
                // a) The same study-PMID combination in both trial regisdtry record and OPubmed record
                // b) The same study-PMID combination in different versions of the study record
                // c) The same PMID beiung used for multiple studies
                // To remove a) and b) a select distinct is done on the current set of unmatched PMID-Study combinations

                pm_tr.FillDistinctTempObjectsTable();

                // Table now has all study id - PMID combinations
                // Match against existing records here and update status and date-time of data fetch

                pm_tr.MatchExistingPMIDLinks();

                // New, unmatched combinations of PMID and studies
                // may have POMIDs completely new to the system, or 
                // neew PMID - study combinations for existing PMIDs

                pm_tr.IdentifyNewPMIDLinkTypes();
                pm_tr.AddNewPMIDStudyLinks();
                pm_tr.AddCompletelyNewPMIDs();
                pm_tr.IdentifyPMIDDataForImport(source_id);

                pm_tr.DropTempPMIDTables();
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
