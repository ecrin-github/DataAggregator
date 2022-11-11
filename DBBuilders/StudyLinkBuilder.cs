using System;
using System.Collections.Generic;
using System.Text;


namespace DataAggregator
{
    public class StudyLinkBuilder
    {
        LinksDataHelper slh;
        string _mdr_connString;
        ICredentials _credentials;
        bool _testing;
        LoggingHelper _loggingHelper;

        public StudyLinkBuilder(ICredentials credentials, string mdr_connString, bool testing, LoggingHelper loggingHelper)
        {
            _credentials = credentials;
            _mdr_connString = mdr_connString;
            _testing = testing;
            _loggingHelper = loggingHelper;
            slh = new LinksDataHelper(_mdr_connString, _loggingHelper);
        }
            
        public void CollectStudyStudyLinks(IEnumerable<Source> sources)
        {
            // Loop through it calling the link helper functions
            // sources are called in 'preference order' starting
            // with clinical trials.gov.

            slh.SetUpTempPreferencesTable(sources);
            slh.SetUpTempLinkCollectorTable();
            slh.SetUpTempLinkSortedTable();

            foreach (Source source in sources)
            {
                // need to populate the ad tables in a test situation with 
                // the relevant data, as the source_conn_string will always 
                // point to 'test' - at least get the study identifiers data!

                if (_testing)
                {
                    slh.TransferTestIdentifiers(source.id);
                }

                // Fetch the study-study links and store them
                // in the Collector table (asuming the source has study data)
                if (source.has_study_tables)
                {
                    string source_conn_string = _credentials.GetConnectionString(source.database_name, _testing);
                    IEnumerable<StudyLink> links = slh.FetchLinks(source.id, source_conn_string);
                    slh.StoreLinksInTempTable(CopyHelpers.links_helper, links);
                }
            }

            // Tidy up common format errors.

            slh.TidyIds1();
            slh.TidyIds2();
            slh.TidyIds3();
        }


        public void CheckStudyStudyLinks(IEnumerable<Source> sources)
        {
            // Create a table with the distinct values obtained 
            // from the aggregation process.

            slh.TransferLinksToSortedTable();
            slh.CreateDistinctSourceLinksTable();

            // Despite earlier cleaning there remains a small number of 
            // study registry Ids that are referenced as'other Ids' but
            // which are errors, which do not correspond to any real studies
            // in the system. These need to be removed, on a source by source basis.

            foreach (Source source in sources)
            {
                if (_testing)
                {
                    // do something - slh.TransferTestIdentifiers(source.id);
                }

                if (source.has_study_tables)
                {
                    string source_conn_string = _credentials.GetConnectionString(source.database_name, _testing);
                    slh.ObtainStudyIds(source.id, source_conn_string, CopyHelpers.studyids_checker); 
                    slh.CheckIdsAgainstSourceStudyIds(source.id);
                }
            }
            slh.DeleteInvalidLinks();
        }


        public void ProcessStudyStudyLinks()
        {
            // Identify and remove studies that have links to more than 1
            // study in another registry - these form study-study relationships
            // rather than simple 1-to-1 study links (though the target links may 
            // need to be updated at the end of the process)

            slh.ProcessGroupedStudies();

            // Identify and repair missing cascade steps
            // Then cascade 'preferred' studies so that the 
            // most preferred always appears on the RHS.

            slh.AddMissingLinks();
            slh.CascadeLinks();
                        
            // Again, identify and remove studies that have links to more than 1
            // study in another registry 
            // A small number (avbout 30) are formed by the cascade process above

            slh.ProcessGroupedStudies();

            // Transfer the resultant set into the 
            // main links table and tidy up

            slh.TransferNewLinksToDataTable();
            slh.UpdateLinksWithStudyIds();
            slh.DropTempTables();
        }


        public void CreateStudyGroupRecords()
        {
            // Select * from nk.linked_study_groups 
            // Use the study_all_ids to insert the study Ids
            // for the linked sources / sd_sids
            slh.AddStudyStudyRelationshipRecords();
        }


    }
}
