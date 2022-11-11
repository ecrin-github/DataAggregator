using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    class RetrieveADDataBuilder
    {

        private int _source_id;
        private string _db_conn;
        private ISource _source;

        public RetrieveADDataBuilder(ISource source)
        {
            _source = source;
            _source_id = source.id;
            _db_conn = source.db_conn;
        }


        public void DeleteExistingADStudyData()
        {
            DeleteData("studies");
            DeleteData("study_identifiers");
            DeleteData("study_titles");
            DeleteData("study_hashes");

            // these are database dependent

            if (_source.has_study_topics) DeleteData("study_topics");
            if (_source.has_study_features) DeleteData("study_features");
            if (_source.has_study_contributors) DeleteData("study_contributors");
            if (_source.has_study_references) DeleteData("study_references");
            if (_source.has_study_relationships) DeleteData("study_relationships");
            if (_source.has_study_links) DeleteData("study_links");
            if (_source.has_study_ipd_available) DeleteData("study_ipd_available");
        }


        public void DeleteExistingADObjectData()
        {
            DeleteData("data_objects");
            DeleteData("object_instances");
            DeleteData("object_titles");
            DeleteData("object_hashes");

            // these are database dependent		

            if (_source.has_object_datasets) DeleteData("object_datasets");
            if (_source.has_object_dates) DeleteData("object_dates");
            if (_source.has_object_relationships) DeleteData("object_relationships");
            if (_source.has_object_rights) DeleteData("object_rights");
            if (_source.has_object_pubmed_set)
            {
                DeleteData("object_contributors");
                DeleteData("object_topics");
                DeleteData("object_comments");
                DeleteData("object_descriptions");
                DeleteData("object_identifiers");
                DeleteData("object_db_links");
                DeleteData("object_publication_types");
            }
        }


        public void RetrieveStudyData()
        {
            ADStudyDataRetriever adr = new ADStudyDataRetriever(_source_id, _db_conn);

            adr.TransferStudies();
            adr.TransferStudyIdentifiers();
            adr.TransferStudyTitles();
            adr.TransferStudyHashes();

            // these are database dependent

            if (_source.has_study_topics) adr.TransferStudyTopics();
            if (_source.has_study_features) adr.TransferStudyFeatures();
            if (_source.has_study_contributors) adr.TransferStudyContributors();
            if (_source.has_study_references) adr.TransferStudyReferences();
            if (_source.has_study_relationships) adr.TransferStudyRelationships();
            if (_source.has_study_countries) adr.TransferStudyCountries();
            if (_source.has_study_locations) adr.TransferStudyLocations();
            if (_source.has_study_links) adr.TransferStudyLinks();
            if (_source.has_study_ipd_available) adr.TransferStudyIPDAvaiable();
        }


        public void RetrieveObjectData()
        {
            ADObjectDataRetriever odr = new ADObjectDataRetriever(_source_id, _db_conn);

            odr.TransferDataObjects();
            odr.TransferObjectInstances();
            odr.TransferObjectTitles();
            odr.TransferObjectHashes();

            // these are database dependent		

            if (_source.has_object_datasets) odr.TransferObjectDatasets();
            if (_source.has_object_dates) odr.TransferObjectDates();
            if (_source.has_object_relationships) odr.TransferObjectRelationships();
            if (_source.has_object_rights) odr.TransferObjectRights();

            if (_source.has_object_pubmed_set)
            {
                odr.TransferObjectContributors();
                odr.TransferObjectTopics();
                odr.TransferObjectComments();
                odr.TransferObjectDescriptions();
                odr.TransferObjectidentifiers();
                odr.TransferObjectDBLinks();
                odr.TransferObjectPublicationTypes();
            }
        }


        private int DeleteData(string table_name)
        {
            int res = 0;
            string sql_string = @"Delete from ad." + table_name;

            using (var conn = new NpgsqlConnection(_db_conn))
            {
                return res = conn.Execute(sql_string);
            }
        }
    }
}
