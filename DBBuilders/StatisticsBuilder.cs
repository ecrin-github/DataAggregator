using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class StatisticsBuilder
    {
        int agg_event_id;
        LoggingDataLayer logging_repo;

        public StatisticsBuilder(LoggingDataLayer _logging_repo, int _agg_event_id)
        {
            logging_repo = _logging_repo;
            agg_event_id = _agg_event_id;
        }


        public void GetStatisticsBySource()
        {
            // get the list of sources
            IEnumerable<Source> sources = logging_repo.RetrieveDataSources();
            // Loop through and...
            // derive a connection string for each source,
            // then get the records contained in each ad table
            // and store it in the databse.

            foreach(Source s in sources)
            {
                string conn_string = logging_repo.FetchConnString(s.database_name);
                SourceSummary sm = new SourceSummary(agg_event_id, s.database_name);

                sm.study_recs = logging_repo.GetRecNum("studies", conn_string);
                sm.study_identifiers_recs = logging_repo.GetRecNum("study_identifiers", conn_string);
                sm.study_titles_recs = logging_repo.GetRecNum("study_titles", conn_string);
                sm.study_contributors_recs = logging_repo.GetRecNum("study_contributors", conn_string);
                sm.study_topics_recs = logging_repo.GetRecNum("study_topics", conn_string);
                sm.study_features_recs = logging_repo.GetRecNum("study_features", conn_string);
                sm.study_references_recs = logging_repo.GetRecNum("study_references", conn_string);
                sm.study_relationships_recs = logging_repo.GetRecNum("study_relationships", conn_string);
                
                sm.data_object_recs = logging_repo.GetRecNum("data_objects", conn_string);
                sm.object_datasets_recs = logging_repo.GetRecNum("object_datasets", conn_string);
                sm.object_instances_recs = logging_repo.GetRecNum("object_instances", conn_string);
                sm.object_titles_recs = logging_repo.GetRecNum("object_titles", conn_string);
                sm.object_dates_recs = logging_repo.GetRecNum("object_dates", conn_string);
                sm.object_contributors_recs = logging_repo.GetRecNum("object_contributors", conn_string);
                sm.object_topics_recs = logging_repo.GetRecNum("object_topics", conn_string);
                sm.object_identifiers_recs = logging_repo.GetRecNum("object_identifiers", conn_string);
                sm.object_descriptions_recs = logging_repo.GetRecNum("object_descriptions", conn_string);
                sm.object_rights_recs = logging_repo.GetRecNum("object_rights", conn_string);
                sm.object_relationships_recs = logging_repo.GetRecNum("object_relationships", conn_string);

                logging_repo.StoreSourceSummary(sm);
            }
        }


        public void GetSummaryStatistics()
        {

        }


        public void GetCoreStatistics()
        {

        }

    }
}
