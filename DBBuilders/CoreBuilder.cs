

namespace DataAggregator
{
    public class CoreBuilder
    {
        private string connString;
        private CoreTableBuilder core_tablebuilder;
        LoggingHelper _loggingHelper;

        public CoreBuilder(string _connString, LoggingHelper loggingHelper)
        {
            _loggingHelper = loggingHelper; 
            connString = _connString;
            core_tablebuilder = new CoreTableBuilder(connString, _loggingHelper);
        }

        public void BuildNewCoreTables()
        {
            core_tablebuilder.create_table_studies();
            core_tablebuilder.create_table_study_identifiers();
            core_tablebuilder.create_table_study_titles();
            core_tablebuilder.create_table_study_topics();
            core_tablebuilder.create_table_study_features();
            core_tablebuilder.create_table_study_contributors();
            core_tablebuilder.create_table_study_relationships();
            core_tablebuilder.create_table_study_countries();
            core_tablebuilder.create_table_study_locations();
            core_tablebuilder.create_table_study_search();

            core_tablebuilder.create_table_data_objects();
            core_tablebuilder.create_table_object_instances();
            core_tablebuilder.create_table_object_titles();
            core_tablebuilder.create_table_object_datasets();
            core_tablebuilder.create_table_object_dates();
            core_tablebuilder.create_table_object_relationships();
            core_tablebuilder.create_table_object_rights();
            core_tablebuilder.create_table_object_contributors();
            core_tablebuilder.create_table_object_topics();
            core_tablebuilder.create_table_object_descriptions();
            core_tablebuilder.create_table_object_identifiers();

            core_tablebuilder.create_table_study_object_links();
        }
    }
}

