﻿

namespace DataAggregator
{
    public class SchemaBuilder
    {
        private string connString;
        private StudyTableBuilder study_tablebuilder;
        private ObjectTableBuilder object_tablebuilder;
        private LinkTableBuilder link_tablebuilder;

        public SchemaBuilder(string _connString)
        {
            connString = _connString;
            study_tablebuilder = new StudyTableBuilder(connString);
            object_tablebuilder = new ObjectTableBuilder(connString);
            link_tablebuilder = new LinkTableBuilder(connString);
        }


        public void BuildNewStudyTables()
        {
            study_tablebuilder.create_table_studies();
            study_tablebuilder.create_table_study_identifiers();
            study_tablebuilder.create_table_study_titles();
            study_tablebuilder.create_table_study_topics();
            study_tablebuilder.create_table_study_features();
            study_tablebuilder.create_table_study_contributors();
            study_tablebuilder.create_table_study_relationships();
            study_tablebuilder.create_table_study_countries();
            study_tablebuilder.create_table_study_locations();
        }


        public void BuildNewObjectTables()
        {
            // these common to all databases

            object_tablebuilder.create_table_data_objects();
            object_tablebuilder.create_table_object_instances();
            object_tablebuilder.create_table_object_titles();

            // these are database dependent		

            object_tablebuilder.create_table_object_datasets();
            object_tablebuilder.create_table_object_dates();
            object_tablebuilder.create_table_object_relationships();
            object_tablebuilder.create_table_object_rights();

            object_tablebuilder.create_table_object_contributors();
            object_tablebuilder.create_table_object_topics();
            object_tablebuilder.create_table_object_descriptions();
            object_tablebuilder.create_table_object_identifiers();
        }


        public void BuildNewLinkTables()
        {
            link_tablebuilder.create_table_linked_study_groups();
            link_tablebuilder.create_table_study_object_links();
            link_tablebuilder.create_table_study_study_links();
        }

    }
}

