using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{

    public class DataObject
    {
        public string file_type { get; set; }
        public int id { get; set; }
        public string doi { get; set; }
        public string display_title { get; set; }
        public string version { get; set; }
        public lookup object_class { get; set; }
        public lookup object_type { get; set; }
        public int? publication_year { get; set; }
        public lookup managing_organisation { get; set; }
        public string lang_code { get; set; }
        public lookup access_type { get; set; }
        public access_details access_details { get; set; }
        public string access_details_url { get; set; }
        public string url_last_checked { get; set; }
        public int? eosc_category { get; set; }
        public string provenance_data { get; set; }

        public record_keys dataset_record_keys { get; set; }
        public deidentification dataset_deident_level { get; set; }
        public consent dataset_consent { get; set; }

        public List<object_identifier> object_identifiers { get; set; }
        public List<object_title> object_titles { get; set; }
        public List<object_contributor> object_contributors { get; set; }
        public List<object_date> object_dates { get; set; }
        public List<object_description> object_descriptions { get; set; }
        public List<object_instance> object_instances { get; set; }
        public List<object_topic> object_topics { get; set; }
        public List<object_right> object_rights { get; set; }
        public List<object_relationship> object_relationships { get; set; }
        public List<int> linked_studies { get; set; }

        public DataObject(int _id, string _doi, string _display_title,
                    lookup _object_class, lookup _object_type, int? _publication_year,
                    lookup _managing_organisation, 
                    lookup _access_type, access_details _access_details)
        {
            id = _id;
            file_type = "data_object";
            doi = _doi;
            display_title = _display_title;
            version = null;
            object_class = _object_class;
            object_type = _object_type;
            publication_year = _publication_year;
            managing_organisation = _managing_organisation;
            access_type = _access_type;
            access_details = _access_details;
        }
    }


    // Many of the properties of the data object are id / name pairs,
    // here known as a lookup as they are derived from lookup tables.

    public class lookup
    {
        public int? id { get; set; }
        public string name { get; set; }

        public lookup(int? _id, string _name)
        {
            id = _id;
            name = _name;
        }
    }

    public class access_details
    {
        public string description { get; set; }
        public string url { get; set; }
        public string url_last_checked { get; set; }

        public access_details(string _description, string _url, string _url_last_checked)
        {
            description = _description;
            url = _url;
            url_last_checked = _url_last_checked;
        }
    }

    public class record_keys
    {
        public int? record_keys_type_id { get; set; }
        public string record_keys_type { get; set; }
        public string record_keys_details { get; set; }

        public record_keys(int? _record_keys_type_id, 
                           string _record_keys_type, string _record_keys_details)
        {
            record_keys_type_id = _record_keys_type_id;
            record_keys_type = _record_keys_type;
            record_keys_details = _record_keys_details;
        }
    }

    public class deidentification
    {
        public int? deident_type_id { get; set; }
        public string deident_type { get; set; }
        public bool deident_direct { get; set; }
        public bool deident_hipaa { get; set; }
        public bool deident_dates { get; set; }
        public bool deident_nonarr { get; set; }
        public bool deident_kanon { get; set; }
        public string deident_details { get; set; }

        public deidentification(int? _deident_type_id, string _deident_type,
                                bool _deident_direct, bool _deident_hipaa,
                                bool _deident_dates, bool _deident_nonarr,
                                bool _deident_kanon, string _deident_details)
        {
            deident_type_id = _deident_type_id;
            deident_type = _deident_type;
            deident_direct = _deident_direct;
            deident_hipaa = _deident_hipaa;
            deident_dates = _deident_dates;
            deident_nonarr = _deident_nonarr;
            deident_kanon = _deident_kanon;
            deident_details = _deident_details;
        }
    }

    public class consent
    {
        public int? consent_type_id { get; set; }
        public string consent_type { get; set; }
        public bool consent_noncommercial { get; set; }
        public bool consent_geog_restrict { get; set; }
        public bool consent_research_type { get; set; }
        public bool consent_genetic_only { get; set; }
        public bool consent_no_methods { get; set; }
        public string consent_details { get; set; }

        public consent(int? _consent_type_id, string _consent_type,
                       bool _consent_noncommercial, bool _consent_geog_restrict,
                       bool _consent_research_type, bool _consent_genetic_only,
                       bool _consent_no_methods, string _consent_details)
        {
            consent_type_id = _consent_type_id;
            consent_type = _consent_type;
            consent_noncommercial = _consent_noncommercial;
            consent_geog_restrict = _consent_geog_restrict;
            consent_research_type = _consent_research_type;
            consent_genetic_only = _consent_genetic_only;
            consent_no_methods = _consent_no_methods;
            consent_details = _consent_details;
        }
    }



    // Corresponds to the repeating composite object_identifier json element
    // and is therefore part of the data object class as a List<> 

    public class object_identifier
    {
        public int id { get; set; }
        public string identifier_value { get; set; }
        public lookup identifier_type { get; set; }
        public lookup identifier_org { get; set; }
        public string identifier_date { get; set; }

        public object_identifier(int _id, lookup _identifier_type, lookup _identifier_org,
                                    string _identifier_value, string _identifier_date)
        {
            id = _id;
            identifier_type = _identifier_type;
            identifier_org = _identifier_org;
            identifier_value = _identifier_value;
            identifier_date = _identifier_date;
        }
    }


    // Corresponds to the repeating composite object_title json element
    // and is therefore part of the data object class as a List<> 

    public class object_title
    {
        public int id { get; set; }
        public lookup title_type { get; set; }
        public string title_text { get; set; }
        public string lang_code { get; set; }
        public string comments { get; set; }

        public object_title(int _id, lookup _title_type, string _title_text,
                        string _lang_code, string _comments)
        {
            id = _id;
            title_type = _title_type;
            title_text = _title_text;
            lang_code = _lang_code;
            comments = _comments;
        }
    }


    // Corresponds to the repeating composite object_date json element
    // and is therefore part of the data object class as a List<> 

    public class object_date
    {
        public int id { get; set; }
        public lookup date_type { get; set; }
        public bool? is_date_range { get; set; }
        public string date_as_string { get; set; }
        public sdate_as_ints start_date { get; set; }
        public edate_as_ints end_date { get; set; }
        public string comments { get; set; }

        public object_date(int _id, lookup _date_type, bool? _is_date_range, string _date_as_string,
                        sdate_as_ints _start_date, edate_as_ints _end_date, string _comments)
        {
            id = _id;
            date_type = _date_type;
            is_date_range = _is_date_range;
            date_as_string = _date_as_string;
            start_date = _start_date;
            end_date = _end_date;
            comments = _comments;
        }
    }


    // A composite date element (the start date) within object_date

    public class sdate_as_ints
    {
        public int? start_year { get; set; }
        public int? start_month { get; set; }
        public int? start_day { get; set; }

        public sdate_as_ints(int? _start_year, int? _start_month, int? _start_day)
        {
            start_year = _start_year;
            start_month = _start_month;
            start_day = _start_day;
        }
    }


    // A composite date element (the end date) within object_date

    public class edate_as_ints
    {
        public int? end_year { get; set; }
        public int? end_month { get; set; }
        public int? end_day { get; set; }

        public edate_as_ints(int? _end_year, int? _end_month, int? _end_day)
        {
            end_year = _end_year;
            end_month = _end_month;
            end_day = _end_day;
        }
    }


    // Corresponds to the repeating composite object_topic json element
    // and is therefore part of the data object class as a List<> 

    public class object_topic
    {
        public int id { get; set; }
        public lookup topic_type_id { get; set; }
        public bool? mesh_coded { get; set; }
        public string topic_code { get; set; }
        public string topic_value { get; set; }
        public string topic_qualcode { get; set; }
        public string topic_qualvalue { get; set; }
        public string original_value { get; set; }

        public object_topic(int _id, lookup _topic_type_id,
                            bool? _mesh_coded, string _topic_code, 
                            string _topic_value,
                            string _topic_qualcode, string _topic_qualvalue,
                            string _original_value)
        {
            id = _id;
            topic_type_id = _topic_type_id;
            mesh_coded = _mesh_coded;
            topic_code = _topic_code;
            topic_value = _topic_value;
            topic_qualcode = _topic_qualcode;
            topic_qualvalue = _topic_qualvalue;
            original_value = _original_value;
        }
        
    }


    // Corresponds to the repeating composite object_contributor json element
    // and is therefore part of the data object class as a List<> 

    public class object_contributor
    {
        public int id { get; set; }
        public lookup contribution_type { get; set; }
        public bool? is_individual { get; set; }
        public lookup organisation { get; set; }
        public individual person { get; set; }

        public object_contributor(int _id, lookup _contribution_type, bool? _is_individual,
                        individual _person, lookup _organisation)
        {
            id = _id;
            contribution_type = _contribution_type;
            is_individual = _is_individual;
            person = _person;
            organisation = _organisation;
        }
    }


    // A composite element within the object_contributor class

    public class individual
    {
        public string family_name { get; set; }
        public string given_name { get; set; }
        public string full_name { get; set; }
        public string orcid { get; set; }
        public string affiliation { get; set; }

        public individual(string _family_name, string _given_name, string _full_name,
                         string _orcid, string _affiliation)
        {
            family_name = _family_name;
            given_name = _given_name;
            full_name = _full_name;
            orcid = _orcid;
            affiliation = _affiliation;
        }
    }


    // Corresponds to the repeating composite object_description json element
    // and is therefore part of the data object class as a List<> 

    public class object_description
    {
        public int id { get; set; }
        public lookup description_type { get; set; }
        public string description_label { get; set; }
        public string description_text { get; set; }
        public string lang_code { get; set; }
        public bool? contains_html { get; set; }
    }


    // Corresponds to the repeating composite object_instance json element
    // and is therefore part of the data object class as a List<> 

    public class object_instance
    {
        public int id { get; set; }
        public lookup repository_org { get; set; }
        public string url { get; set; }
        public bool? url_direct_access { get; set; }
        public string url_last_checked { get; set; }
        public lookup resource_type { get; set; }
        public float? resource_size { get; set; }
        public string resource_size_units { get; set; }

        public object_instance(int _id, lookup _repository_org, string _url,
                        bool? _url_direct_access, string _url_last_checked,
                        lookup _resource_type, float? _resource_size, string _resource_size_units)
        {
            id = _id;
            repository_org = _repository_org;
            url = _url;
            url_direct_access = _url_direct_access;
            url_last_checked = _url_last_checked;
            resource_type = _resource_type;
            resource_size = _resource_size;
            resource_size_units = _resource_size_units;
        }
    }


    // Corresponds to the repeating composite object_right json element
    // and is therefore part of the data object class as a List<> 

    public class object_right
    {
        public int id { get; set; }
        public string rights_name { get; set; }
        public string rights_uri { get; set; }
        public string comments { get; set; }
    }


    // Corresponds to the repeating composite related_object json element
    // and is therefore part of the data object class as a List<> 

    public class object_relationship
    {
        public int id { get; set; }
        public lookup relationship_type { get; set; }
        public int target_object_id { get; set; }
    }


    // These classes correspond to the data returned by the various
    // 'Fetch' routines in the DataLayer class - each DB class matches
    // the structure of the corresponding table.
    // They represent the form in which data is presented to the Processor's CreateObject routine.
    // That routine modifies the data as and when necessary, and then
    // aggregates it, to make it conform to the structure required in the final 
    // data object class that is delivered back to the main program.

    [Table("core.data_objects")]
    public class DBDataObject
    {
        public int id { get; set; }
        public string doi { get; set; }
        public string display_title { get; set; }
        public string version { get; set; }
        public int? object_class_id { get; set; }
        public string object_class { get; set; }
        public int? object_type_id { get; set; }
        public string object_type { get; set; }
        public int? publication_year { get; set; }
        public string lang_code { get; set; }
        public int? managing_org_id { get; set; }
        public string managing_org { get; set; }
        public int? access_type_id { get; set; }
        public string access_type { get; set; }
        public string access_details { get; set; }
        public string access_details_url { get; set; }
        public string url_last_checked { get; set; }
        public int eosc_category { get; set; }
        public bool? add_study_contribs { get; set; }
        public bool? add_study_topics { get; set; }
        public string provenance_string { get; set; }
    }


    [Table("core.dataset_properties")]
    public class DBDatasetProperties
    {
        public int id { get; set; }
        public int? record_keys_type_id { get; set; }
        public string record_keys_type { get; set; }
        public string record_keys_details { get; set; }
        public int? deident_type_id { get; set; }
        public string deident_type { get; set; }
        public bool deident_direct { get; set; }
        public bool deident_hipaa { get; set; }
        public bool deident_dates { get; set; }
        public bool deident_nonarr { get; set; }
        public bool deident_kanon { get; set; }
        public string deident_details { get; set; }
        public int? consent_type_id { get; set; }
        public string consent_type { get; set; }
        public bool consent_noncommercial { get; set; }
        public bool consent_geog_restrict { get; set; }
        public bool consent_research_type { get; set; }
        public bool consent_genetic_only { get; set; }
        public bool consent_no_methods { get; set; }
        public string consent_details { get; set; }
    }


    [Table("core.object_instances")]
    public class DBObjectInstance
    {
        public int id { get; set; }
        public int instance_type_id { get; set; }
        public string instance_type { get; set; }
        public int? repository_org_id { get; set; }
        public string repository_org { get; set; }
        public string url { get; set; }
        public bool? url_accessible { get; set; }
        public string url_last_checked { get; set; }
        public int? resource_type_id { get; set; }
        public string resource_type { get; set; }
        public float? resource_size { get; set; }
        public string resource_size_units { get; set; }
        public string comments { get; set; }
    }


    [Table("core.object_titles")]
    public class DBObjectTitle
    {
        public int id { get; set; }
        public int? title_type_id { get; set; }
        public string title_type { get; set; }
        public string title_text { get; set; }
        public string lang_code { get; set; }
        public string comments { get; set; }
    }


    [Table("core.object_dates")]
    public class DBObjectDate
    {
        public int id { get; set; }
        public int? date_type_id { get; set; }
        public string date_type { get; set; }
        public bool? is_date_range { get; set; }
        public string date_as_string { get; set; }
        public int? start_year { get; set; }
        public int? start_month { get; set; }
        public int? start_day { get; set; }
        public int? end_year { get; set; }
        public int? end_month { get; set; }
        public int? end_day { get; set; }
        public string comments { get; set; }
    }

    [Table("core.object_identifiers")]
    public class DBObjectIdentifier
    {
        public int id { get; set; }
        public string identifier_value { get; set; }
        public int? identifier_type_id { get; set; }
        public string identifier_type { get; set; }
        public int? identifier_org_id { get; set; }
        public string identifier_org { get; set; }
        public string identifier_date { get; set; }
    }


    [Table("core.object_contributors")]
    public class DBObjectContributor
    {
        public int id { get; set; }
        public int? contrib_type_id { get; set; }
        public string contrib_type { get; set; }
        public bool is_individual { get; set; }
        public int? organisation_id { get; set; }
        public string organisation_name { get; set; }
        public int? person_id { get; set; }
        public string person_given_name { get; set; }
        public string person_family_name { get; set; }
        public string person_full_name { get; set; }
        public string public_identifier { get; set; }
        public string identifier_type { get; set; }
        public string affiliation { get; set; }
        public string affil_org_id { get; set; }
        public string affil_org_id_type { get; set; }
    }


    [Table("core.object_topics")]
    public class DBObjectTopic
    {
        public int id { get; set; }
        public int? topic_type_id { get; set; }
        public string topic_type { get; set; }
        public bool mesh_coded { get; set; }
        public string topic_code { get; set; }
        public string topic_value { get; set; }
        public string topic_qualcode { get; set; }
        public string topic_qualvalue { get; set; }
        public int original_ct_id { get; set; }
        public string original_ct { get; set; }
        public string original_ct_code { get; set; }
        public string original_value { get; set; }
        public string comments { get; set; }
    }


    [Table("core.object_descriptions")]
    public class DBObjectDescription
    {
        public int id { get; set; }
        public int? description_type_id { get; set; }
        public string description_type { get; set; }
        public string label { get; set; }
        public string description_text { get; set; }
        public string lang_code { get; set; }
        public bool? contains_html { get; set; }
    }


    [Table("core.object_relationships")]
    public class DBObjectRelationship
    {
        public int id { get; set; }
        public int? relationship_type_id { get; set; }
        public string relationship_type { get; set; }
        public int target_object_id { get; set; }
    }


    [Table("core.object_rights")]
    public class DBObjectRight
    {
        public int id { get; set; }
        public string rights_name { get; set; }
        public string rights_uri { get; set; }
        public string comments { get; set; }
    }


    [Table("core.study_object_links")]
    public class DBObjectStudyLink
    {
        public int id { get; set; }
        public int study_id { get; set; }
        public int object_id { get; set; }
    }

}
