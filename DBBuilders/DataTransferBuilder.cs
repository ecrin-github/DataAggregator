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


        public DataTransferBuilder(Source _source, string _schema_name, string _source_conn_string)
        {
            source = _source;
            DataLayer repo = new DataLayer("mdr");
            st_tr = new StudyDataTransferrer(repo);
            ob_tr = new ObjectDataTransferrer(repo);
            schema_name = _schema_name;
            source_conn_string = _source_conn_string;
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
            IEnumerable<StudyIds> study_ids = st_tr.FetchStudyIds(source.id, source_conn_string);
            StringHelpers.SendFeedback("Study Ids obtained");
            st_tr.StoreStudyIds(IdCopyHelpers.study_ids_helper, study_ids);
            StringHelpers.SendFeedback("Study Ids stored");

            // Do the check of the temp table ids against the study_study links.
            // Change the table to reflect the 'preferred' Ids.
            // Back load the correct study ids into the temporary table.
            
            st_tr.CheckStudyLinks();
            st_tr.UpdateAllStudyIdsTable(source.id);
            StringHelpers.SendFeedback("Study Ids processed");
        }


        public void TransferStudyData()
        {
            st_tr.LoadStudies(schema_name);
            // st_tr.LoadStudyIdentifiers(schema_name);
            // st_tr.LoadStudyTitles(schema_name);
            //if (source.has_study_contributors) st_tr.LoadStudyContributors(schema_name);
            //if (source.has_study_topics) st_tr.LoadStudyTopics(schema_name);
            //if (source.has_study_features) st_tr.LoadStudyFeatures(schema_name);
            //if (source.has_object_relationships) st_tr.LoadStudyRelationShips(schema_name);
            st_tr.DropTempStudyIdsTable();
        }


        public void ProcessStudyObjectIds()
        {
            // Set up temp tables and fill the first with the sd_oids, 
            // parent sd_sids, dates of data fetch, of the objects in 
            // the source database.

            ob_tr.SetUpTempObjectIdsTables();
            IEnumerable<ObjectIds> object_ids = ob_tr.FetchObjectIds(source.id);
            ob_tr.StoreObjectIds(IdCopyHelpers.object_ids_helper, object_ids);

            // Update the object parent ids against the all_ids_studies table

            ob_tr.UpdateObjectStudyIds(source.id);

            // Carry out a check for (currently very rare) duplicate
            // objects (i.e. that have been imported before with the data 
            // from another source). [TO IMPLEMENT}
            ob_tr.CheckStudyObjectsForDuplicates(source.id);

            // Update the database all objects ids table and derive a 
            // small table that lists the object Ids for all objects
            ob_tr.UpdateAllObjectIdsTable(source.id);
            ob_tr.FillObjectsToAddTable(source.id);

        }


        public void ProcessStandaloneObjectIds()
        {
            ob_tr.SetUpTempObjectIdsTables();

            // process the data using available object-study links
            // (may be multiple study links per object)
            // exact process likely to differ with different standalone
            // object sources - at the moment onlyy PubMed in this category

            if (source.id == 100135)
            {
                // get the source study-pmid link table data (created at harvest)
                // Probably better done at import!
                IEnumerable<ObjectIds> object_ids = ob_tr.FetchPMIDs(source.id);
                ob_tr.StoreObjectIds(IdCopyHelpers.object_ids_helper, object_ids);
                
                // Use cascade on study-study link table to get preferred sd_sid
                // then drop any resulting duplicates from study-pmid table
                ob_tr.InputPreferredSDSIDS();

                // add in study-pmid links to all_ids_objects
                ob_tr.AddPMIDLinksToAllObjectIdsTable();

                // use min of ids to set all object ids the same for the same pmid
                ob_tr.ResetIdsOfDuplicatedPMIDs(source.id);

                // make new table of distinct pmids to add                 
                ob_tr.FillObjectsToAddTable(source.id);

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


        public void TransferLinkData()
        {

        }


        public void TransferCoreStudyData()
        {

        }

         
        public void TransferCoreObjectData()
        {

        }


        public void TransferCoreLinkData()
        {

        }


        

    }
}
