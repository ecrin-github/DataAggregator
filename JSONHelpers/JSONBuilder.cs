using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class JSONBuilder
    {
        JSONHelper jh;

        public JSONBuilder(string _connString)
        {
            jh = new JSONHelper(_connString);
        }

        public void CreateJSONTables()
        {
            jh.CreateJSONTables();
        }

        /*
        public void CreateJSONStudyData()
        {
            jh.InsertStudyJSON();
            jh.InsertStudyIdentifierJSON();
            jh.InsertStudyTitleJSON();
            jh.InsertStudyFeatureJSON();
            jh.InsertStudyTopicJSON();
            jh.InsertStudyRelationshipJSON();
            jh.InsertStudyObjectLinkJSON();
        }

        public void CreateJSONObjectData()
        {
            jh.InsertObjectJSON();
            jh.InsertObjectDatasetJSON();
            jh.InsertObjectTitleJSON();
            jh.InsertObjecInstanceJSON();
            jh.InsertObjectDateJSON();
            jh.InsertObjectContributorJSON();
            jh.InsertObjectTopicJSON();
            jh.InsertObjectDescriptionJSON();
            jh.InsertObjectRelationshipJSON();
            jh.InsertObjectIdentifierJSON();            
            jh.InsertObjectRightsJSON();
            jh.InsertObjectStudyLinkJSON();
        }
        */

        public void CreateJSONStudyData()
        {
            JSONDataLayer repo = new JSONDataLayer();
            int min_id = repo.FetchMinId("studies");
            int max_id = repo.FetchMaxId("studies");

            jh.LoopThroughStudyRecords(repo, min_id, max_id);


            // instantiate a (json) study object


            // fill it with study level details


            // add the study identifier details


            // add the study title details


            // add the study feature details


            // add the study topic details


            // add the study relationships, if any


            // add the related objects data


            // store the resulting object as json in the json field


            // record progress

        }


        public void CreateJSONObjectData()
        {
            

        }



        public void CreateJSONStudyFiles()
        {

        }

        public void CreateJSONObjectFiles()
        {

        }
    }
}
