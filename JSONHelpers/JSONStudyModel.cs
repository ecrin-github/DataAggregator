using Dapper.Contrib.Extensions;
using System.Collections.Generic;

namespace DataAggregator
{
    public class JSONStudy
    {
        public string file_type { get; set; }
        public int id { get; set; }
        public string display_title { get; set; }
        public string brief_description { get; set; }
        public string data_sharing_statement { get; set; }
        public lookup study_type { get; set; }
        public lookup study_status { get; set; }
        public string study_enrolment { get; set; }
        public lookup study_gender_elig { get; set; }
        public age_param min_age { get; set; }
        public age_param max_age { get; set; }
        public year_month start_time { get; set; }
        public string provenance_string { get; set; }

        public List<study_identifier> study_identifiers { get; set; }
        public List<study_title> study_titles { get; set; }
        public List<study_topic> study_topics { get; set; }
        public List<study_feature> study_features { get; set; }
        public List<study_contributor> study_contributors { get; set; }
        public List<study_country> study_countries { get; set; }
        public List<study_location> study_locations { get; set; }
        public List<study_relationship> study_relationships { get; set; }
        public List<int> linked_data_objects { get; set; }

        public JSONStudy(int _id, string _display_title,
                         string _brief_description, string _data_sharing_statement,
                         lookup _study_type, lookup _study_status, string _study_enrolment,
                         lookup _study_gender_elig, age_param _min_age, age_param _max_age,
                         year_month _start_time, string _provenance_string)
        {
            file_type = "study";
            id = _id;
            display_title = _display_title;
            brief_description = _brief_description;
            data_sharing_statement = _data_sharing_statement;
            study_type = _study_type;
            study_status = _study_status;
            study_enrolment = _study_enrolment;
            study_gender_elig = _study_gender_elig;
            min_age = _min_age;
            max_age = _max_age;
            start_time = _start_time;
            provenance_string = _provenance_string;
        }
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


    public class geonames_entity
    {
        public int? geonames_id { get; set; }
        public string name { get; set; }

        public geonames_entity(int? _geonames_id, string _name)
        {
            geonames_id = _geonames_id;
            name = _name;
        }
    }


    public class year_month
    {
        public int? year { get; set; }
        public int? month { get; set; }

        public year_month(int? _year, int? _month)
        {
            year = _year;
            month = _month;
        }
    }


    public class study_identifier
    {
        public int id { get; set; }
        public string identifier_value { get; set; }
        public lookup identifier_type { get; set; }
        public organisation identifier_org { get; set; }
        public string identifier_date { get; set; }
        public string identifier_link { get; set; }

        public study_identifier(int _id, string _identifier_value,
                           lookup _identifier_type, organisation _identifier_org,
                           string _identifier_date, string _identifier_link)
        {
            id = _id;
            identifier_value = _identifier_value;
            identifier_type = _identifier_type;
            identifier_org = _identifier_org;
            identifier_date = _identifier_date;
            identifier_link = _identifier_link;
        }
    }

    public class study_title
    {
        public int id { get; set; }
        public lookup title_type { get; set; }
        public string title_text { get; set; }
        public string lang_code { get; set; }
        public string comments { get; set; }

        public study_title(int _id, lookup _title_type,
                           string _title_text, string _lang_code,
                           string _comments)
        {
            id = _id;
            title_type = _title_type;
            title_text = _title_text;
            lang_code = _lang_code;
            comments = _comments;
        }
    }


    public class study_topic
    {
        public int id { get; set; }
        public lookup topic_type { get; set; }
        public bool mesh_coded { get; set; }
        public string mesh_code { get; set; }
        public string mesh_value { get; set; }
        public int original_ct_id { get; set; }
        public string original_ct_code { get; set; }
        public string original_value { get; set; }

        public study_topic(int _id, lookup _topic_type,
                             bool _mesh_coded, string _mesh_code,
                             string _mesh_value, int _original_ct_id,
                             string _original_ct_code, string _original_value)
        {
            id = _id;
            topic_type = _topic_type;
            mesh_coded = _mesh_coded;
            mesh_code = _mesh_code;
            mesh_value = _mesh_value;
            original_ct_id = _original_ct_id;
            original_ct_code = _original_ct_code;
            original_value = _original_value;
        }
    } 

    public class study_feature
    {
        public int id { get; set; }
        public lookup feature_type { get; set; }
        public lookup feature_value { get; set; }

        public study_feature(int _id, lookup _feature_type,
                                  lookup _feature_value)
        {
            id = _id;
            feature_type = _feature_type;
            feature_value = _feature_value;
        }
    }


    public class study_contributor
    {
        public int id { get; set; }
        public lookup contribution_type { get; set; }
        public bool? is_individual { get; set; }
        public individual person { get; set; }
        public organisation organisation { get; set; }

        public study_contributor(int _id, lookup _contribution_type, bool? _is_individual,
                        individual _person, organisation _organisation)
        {
            id = _id;
            contribution_type = _contribution_type;
            is_individual = _is_individual;
            person = _person;
            organisation = _organisation;
        }
    }


    public class study_country
    {
        public int id { get; set; }
        public geonames_entity country { get; set; }
        public lookup status { get; set; }

        public study_country(int _id, geonames_entity _country, lookup _status)
        {
            id = _id;
            country = _country;
            status = _status;
        }
    }


    public class study_location
    {
        public int id { get; set; }
        public organisation facility { get; set; }
        public geonames_entity city { get; set; }
        public geonames_entity country { get; set; }
        public lookup status { get; set; }

        public study_location(int _id, organisation _facility, geonames_entity _city, 
            geonames_entity _country, lookup _status)
        {
            id = _id;
            facility = _facility;
            city = _city;
            country = _country;
            status = _status;
        }
    }



    public class study_relationship
    {
        public int id { get; set; }
        public lookup relationship_type { get; set; }
        public int target_study_id { get; set; }

        public study_relationship(int _id, lookup _relationship_type,
                                  int _target_study_id)
        {
            id = _id;
            relationship_type = _relationship_type;
            target_study_id = _target_study_id;
        }
    }

    public class study_object_link
    {
        public int id { get; set; }
        public int study_id { get; set; }
        public int object_id { get; set; }
    }


    [Table("core.studies")]
    public class DBStudy
    {
        public int id { get; set; }
        public string display_title { get; set; }
        public string title_lang_code { get; set; }
        public string brief_description { get; set; }
        public string data_sharing_statement { get; set; }
        public int? study_type_id { get; set; }
        public string study_type { get; set; }
        public int? study_status_id { get; set; }
        public string study_status { get; set; }
        public string study_enrolment { get; set; }
        public int? study_gender_elig_id { get; set; }
        public string study_gender_elig { get; set; }
        public int? min_age { get; set; }
        public int? min_age_units_id { get; set; }
        public string min_age_units { get; set; }
        public int? max_age { get; set; }
        public int? max_age_units_id { get; set; }
        public string max_age_units { get; set; }
        public int? study_start_year { get; set; }
        public int? study_start_month { get; set; }
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
        public string identifier_org_ror_id { get; set; }
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
        public string mesh_code { get; set; }
        public string mesh_value { get; set; }
        public int original_ct_id { get; set; }
        public string original_ct_code { get; set; }
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


    [Table("core.study_contributors")]
    public class DBStudyContributor
    {
        public int id { get; set; }
        public int? contrib_type_id { get; set; }
        public string contrib_type { get; set; }
        public bool? is_individual { get; set; }
        public int? person_id { get; set; }
        public string person_given_name { get; set; }
        public string person_family_name { get; set; }
        public string person_full_name { get; set; }
        public string orcid_id { get; set; }
        public string person_affiliation { get; set; }
        public int? organisation_id { get; set; }
        public string organisation_name { get; set; }
        public string organisation_ror_id { get; set; }
    }


    [Table("core.study_countries")]
    public class DBStudyCountry
    {
        public int id { get; set; }
        public int? country_id { get; set; }
        public string country_name { get; set; }
        public int? status_id { get; set; }
        public string status_type { get; set; }
    }


    [Table("core.study_locations")]
    public class DBStudyLocation
    {
        public int id { get; set; }
        public int? facility_org_id { get; set; }
        public string facility { get; set; }
        public string facility_ror_id { get; set; }
        public int? city_id { get; set; }
        public string city_name { get; set; }
        public int? country_id { get; set; }
        public string country_name { get; set; }
        public int? status_id { get; set; }
        public string status_type { get; set; }
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


    [Table("core.studies_json")]
    public class DBStudyJSON
    {
        public int id { get; set; }
        public string json { get; set; }

        public DBStudyJSON(int _id, string _json)
        {
            id = _id;
            json = _json;
        }
    }

}
