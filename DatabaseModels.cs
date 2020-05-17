using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
	public class StudyData
	{
		public int id { get; set; }
		public string display_title { get; set; }
		public string title_lang_code { get; set; }
		public string brief_description { get; set; }
		public string data_sharing_statement { get; set; }
		public int? study_start_year { get; set; }
		public int? study_start_month { get; set; }
		public int? study_type_id { get; set; }
		public int? study_status_id { get; set; }
		public int? study_enrolment { get; set; }
		public int? study_gender_elig_id { get; set; }
		public int? min_age { get; set; }
		public int? min_age_units_id { get; set; }
		public int? max_age { get; set; }
		public int? max_age_units_id { get; set; }
		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }
	}


	public class StudyTitle
	{
		public int study_id { get; set; }
		public string title_text { get; set; }
		public int? title_type_id { get; set; }
		public string title_lang_code { get; set; }
		public int lang_usage_id  { get; set; }
	    public bool is_default { get; set; }
		public string comments { get; set; }

		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }
	}


	public class StudyContributor
	{
		public int study_id { get; set; }
		public int? contrib_type_id { get; set; }
		public bool is_individual { get; set; }
		public int? organisation_id { get; set; }
		public string organisation_name { get; set; }
		public int? person_id { get; set; }
		public string person_given_name { get; set; }
		public string person_family_name { get; set; }
		public string person_full_name { get; set; }
		public string person_identifier { get; set; }
		public string identifier_type { get; set; }
		public string person_affiliation { get; set; }
		public string affil_org_id { get; set; }
		public string affil_org_id_type { get; set; }

		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }
	}


	public class StudyRelationship
	{
		public int study_id { get; set; }
		public int relationship_type_id { get; set; }
		public string target_sd_id { get; set; }
		public int target_id { get; set; }
		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }
	}


	public class StudyIdentifier
	{
		public int study_id { get; set; }
		public string identifier_value { get; set; }
		public int? identifier_type_id { get; set; }
		public int? identifier_org_id { get; set; }
		public string identifier_org { get; set; }
		public string identifier_date { get; set; }
		public string identifier_link { get; set; }

		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }
	}


	public class StudyTopic
	{
		public int study_id { get; set; }
		public int topic_type_id { get; set; }
		public string topic_type { get; set; }
		public string topic_value { get; set; }
		public int topic_ct_id { get; set; }
		public string topic_ct { get; set; }
		public string topic_ct_code { get; set; }
		public string where_found { get; set; }

		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }
	}


	public class StudyFeature
	{
		public string sd_id { get; set; }
		public int? feature_type_id { get; set; }
		public string feature_type { get; set; }
		public int? feature_value_id { get; set; }
		public string feature_value { get; set; }

		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }
	}


	public class DataObject
	{
		public int id { get; set; }
		public string display_title { get; set; }
		public string doi { get; set; }
		public int doi_status_id { get; set; }
		public int? publication_year { get; set; }
		public int object_class_id { get; set; }
		public int? object_type_id { get; set; }
		public int? managing_org_id { get; set; }
		public string managing_org { get; set; }
		public int? access_type_id { get; set; }
		public string access_details { get; set; }
		public string access_details_url { get; set; }
		public DateTime? url_last_checked { get; set; }
		public bool add_study_contribs { get; set; }
		public bool add_study_topics { get; set; }
		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }
	}


	public class DataSetProperties
	{
		public int id { get; set; }
		public int? record_keys_type_id { get; set; }
		public string record_keys_details { get; set; }
		public int? identifiers_type_id { get; set; }
		public string identifiers_details { get; set; }
		public int? consents_type_id { get; set; }
		public string consents_details { get; set; }
		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }

	}


	public class DataObjectTitle
	{
		public int object_id { get; set; }
		public string title_text { get; set; }
		public int? title_type_id { get; set; }
		public string title_lang_code { get; set; }
		public int lang_usage_id { get; set; }
		public bool is_default { get; set; }
		public string comments { get; set; }
		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }
	}


	public class DataObjectInstance
	{
		public int object_id { get; set; }
		public int? instance_type_id { get; set; }
		public string instance_type { get; set; }
		public int? repository_org_id { get; set; }
		public string repository_org { get; set; }
		public string url { get; set; }
		public bool url_accessible { get; set; }
		public DateTime? url_last_checked { get; set; }
		public int? resource_type_id { get; set; }
		public string resource_size { get; set; }
		public string resource_size_units { get; set; }
		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }

	}


	public class DataObjectDate
	{
		public int object_id { get; set; }
		public int date_type_id { get; set; }
		public string date_type { get; set; }
		public string date_as_string { get; set; }
		public bool is_date_range { get; set; }
		public int? start_year { get; set; }
		public int? start_month { get; set; }
		public int? start_day { get; set; }
		public int? end_year { get; set; }
		public int? end_month { get; set; }
		public int? end_day { get; set; }
		public string details { get; set; }

		public DateTime? date_of_data { get; set; }
		public int record_status_id { get; set; }
	}

	Object languages too!
}
