using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class JSONStudy
    {
        public string file_type { get; set; }
        public int id { get; set; }
        public string display_title { get; set; }
        public text_block brief_description { get; set; }
        public text_block data_sharing_statement { get; set; }
        public lookup study_type { get; set; }
        public lookup study_status { get; set; }
        public int? study_enrolment { get; set; }
        public lookup study_gender_elig { get; set; }
        public age_param min_age { get; set; }
        public age_param max_age { get; set; }
        public string provenance_string { get; set; }

        public List<study_identifier> identifiers { get; set; }
        public List<study_title> titles { get; set; }
        public List<study_topic> topics { get; set; }
        public List<study_feature> features { get; set; }
        public List<study_relationship> relationships { get; set; }
        public List<int> linked_objects { get; set; }
    }

    public class text_block
    {
        public string text{ get; set; }
        public bool? contains_html { get; set; }
    }

    public class age_param
    {
        public int? value { get; set; }
        public int? unit_id { get; set; }
        public string unit_name { get; set; }

        public age_param(int? _value, int? _unit_id, string _unit_name)
        {
            value = _value;
            unit_id = _unit_id;
            unit_name = _unit_name;
        }
    }

    public class study_identifier
    {
        public int id { get; set; }
        public string identifier_value { get; set; }
        public lookup identifier_type { get; set; }
        public lookup identifier_org { get; set; }
        public string identifier_date { get; set; }
        public string identifier_link { get; set; }
    }

    public class study_title
    {
        public int id { get; set; }
        public lookup title_type { get; set; }
        public string title_text { get; set; }
        public string lang_code { get; set; }
        public string comments { get; set; }
   
    }

    public class study_topic
    {
        public int id { get; set; }
        public lookup topic_type { get; set; }
        public bool mesh_coded { get; set; }
        public string topic_code { get; set; }
        public string topic_value { get; set; }
        public string topic_qualcode { get; set; }
        public string topic_qualvalue { get; set; }
        public string original_value { get; set; }
    }

    public class study_feature
    {
        public int id { get; set; }
        public lookup feature_type { get; set; }
        public lookup feature_value { get; set; }
    }

    public class study_relationship
    {
        public int id { get; set; }
        public lookup relationship_type { get; set; }
        public int target_study_id { get; set; }
    }


    [Table("core.studies")]
    public class DBStudy
    {
        public int id { get; set; }
        public string display_title { get; set; }
        public string title_lang_code { get; set; }
        public string brief_description { get; set; }
        public bool? bd_contains_html { get; set; }        
        public string data_sharing_statement { get; set; }
        public bool? dss_contains_html { get; set; }
        public int? study_type_id { get; set; }
        public string study_type { get; set; }
        public int? study_status_id { get; set; }
        public string study_status { get; set; }
        public int? study_enrolment { get; set; }
        public int? study_gender_elig_id { get; set; }
        public string study_gender_elig { get; set; }
        public int? min_age { get; set; }
        public int? min_age_units_id { get; set; }
        public string min_age_units { get; set; }
        public int? max_age { get; set; }
        public int? max_age_units_id { get; set; }
        public string max_age_units { get; set; }
        public string provenance_string { get; set; }
    }

    [Table("core.study_identifiers")]
    public class DBStudyIdentifier
    {
        public int id { get; set; }
        public string identifier_value { get; set; }
        public int? identifier_type_id { get; set; }
        public string identifier_type { get; set; }
        public int? identifier_org_id { get; set; }
        public string identifier_org { get; set; }
        public string identifier_date { get; set; }
        public string identifier_link { get; set; }
    }


    [Table("core.study_titles")]
    public class DBStudyTitle
    {
        public int id { get; set; }
        public int? title_type_id { get; set; }
        public string title_type { get; set; }
        public string title_text { get; set; }
        public string lang_code { get; set; }
        public string comments { get; set; }
    }



    [Table("core.study_topics")]
    public class DBStudyTopic
    {
        public int id { get; set; }
        public int? topic_type_id { get; set; }
        public string topic_type { get; set; }
        public bool mesh_coded { get; set; }
        public string topic_code { get; set; }
        public string topic_value { get; set; }
        public string topic_qualcode { get; set; }
        public string topic_qualvalue { get; set; }
        public string original_value { get; set; }
    }


    [Table("core.study_features")]
    public class DBStudyFeature
    {
        public int id { get; set; }
        public int? feature_type_id { get; set; }
        public string feature_type { get; set; }
        public int? feature_value_id { get; set; }
        public string feature_value { get; set; }
    }


    [Table("core.study_relationships")]
    public class DBStudyRelationship
    {
        public int id { get; set; }
        public int? relationship_type_id { get; set; }
        public string relationship_type { get; set; }
        public int target_study_id { get; set; }
    }


    [Table("core.study_object_links")]
    public class DBStudyObjectLink
    {
        public int id { get; set; }
        public int study_id { get; set; }
        public int object_id { get; set; }
    }



}
