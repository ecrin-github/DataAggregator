using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class StudyLinkBuilder
    {
        DataLayer repo;
        LinksDataHelper slh;
        LoggingDataLayer logging_repo;

        public StudyLinkBuilder(DataLayer _repo, LoggingDataLayer _logging_repo)
        {
           repo = _repo;
           logging_repo = _logging_repo;
           slh = new LinksDataHelper(repo, logging_repo);
        }
            
        public void CollectStudyStudyLinks(IEnumerable<Source> sources)
        {
            // Loop through it calling the link helper functions
            // sources are called in 'preference order' starting
            // with clinical trials.gov.

            slh.SetUpTempPreferencesTable(sources);
            slh.SetUpTempLinkCollectorTable();
            slh.SetUpTempLinkSortedTable();
            foreach (Source s in sources)
            {
                // Fetch the study-study links and store them
                // in the Collector table 
                IEnumerable<StudyLink> links = slh.FetchLinks(s.id, s.database_name);
                slh.StoreLinksInTempTable(CopyHelpers.links_helper, links);
            }

            // Tidy up common format errors and then store links in the 'correct' 
            // arrangement, i.e. lower rated sources in the 'preferred' fields.

            slh.TidyIds1();
            slh.TidyIds2();
            slh.TidyIds3();
        }


        public void ProcessStudyStudyLinks()
        {
            // Create a table with the distinct values obtained 
            // from the aggregation process.

            slh.TransferLinksToSortedTable();
            slh.CreateDistinctSourceLinksTable();

            // Identify and remove studies that have links to more than 1
            // study in another registry - these form study-study relationships
            // rather than simple 1-to-1 study links (though the target links may 
            // need to be updated at the end of the process)

            slh.IdentifyGroupedStudies();
            slh.ExtractGroupedStudiess();
            slh.DeleteGroupedStudyLinkRecords();

            // Cascade 'preferred' studies so that the 
            // most preferred always appears on the RHS
            // Identify and repair missing cascade steps
            // then 're-cascade' links.

            //slh.CascadeLinksInDistinctLinksTable();
            slh.ManageIncompleteLinks();
            slh.CascadeLinksInDistinctLinksTable();

            // Transfer the (distinct) resultant set into the 
            // main links table and tidy up
            slh.TransferNewLinksToDataTable();
            slh.DropTempTables();
        }


        public void CreateStudyGroupRecords()
        {
            // Select* from nk.linked_study_groups 
            // Use the study_all_ids to insert the study Ids
            // for the linked sources / sd_sids
            slh.AddStudyStudyRelationshipRecords();
        }


    }
}
