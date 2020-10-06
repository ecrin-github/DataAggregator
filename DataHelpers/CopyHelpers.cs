using System;
using System.Collections.Generic;
using System.Text;
using PostgreSQLCopyHelper;

namespace DataAggregator
{
	public static class CopyHelpers
	{
		public static PostgreSQLCopyHelper<StudyData> study_data_helper =
			   new PostgreSQLCopyHelper<StudyData>("st", "studies")
				   .MapInteger("id", x => x.id)
				   .MapVarchar("display_title", x => x.display_title)
				   .MapVarchar("title_lang_code", x => x.title_lang_code)
				   .MapVarchar("brief_description", x => x.brief_description)
				   .MapVarchar("data_sharing_statement", x => x.data_sharing_statement)
				   .MapInteger("study_start_year", x => x.study_start_year)
				   .MapInteger("study_start_month", x => x.study_start_month)
				   .MapInteger("study_type_id", x => x.study_type_id)
				   .MapInteger("study_status_id", x => x.study_status_id)
				   .MapInteger("study_enrolment", x => x.study_enrolment)
				   .MapInteger("study_gender_elig_id", x => x.study_gender_elig_id)
				   .MapInteger("min_age", x => x.min_age)
				   .MapInteger("min_age_units_id", x => x.min_age_units_id)
				   .MapInteger("max_age", x => x.max_age)
				   .MapInteger("max_age_units_id", x => x.max_age_units_id)
				   .MapTimeStampTz("date_of_data", x => x.date_of_data)
				   .MapInteger("record_status_id", x => x.record_status_id);


		public static PostgreSQLCopyHelper<StudyIdentifier> study_ids_helper =
			new PostgreSQLCopyHelper<StudyIdentifier>("st", "study_identifiers")
				.MapInteger("study_id", x => x.study_id)
				.MapVarchar("identifier_value", x => x.identifier_value)
				.MapInteger("identifier_type_id", x => x.identifier_type_id)
				.MapInteger("identifier_org_id", x => x.identifier_org_id)
				.MapVarchar("identifier_org", x => x.identifier_org)
				.MapVarchar("identifier_date", x => x.identifier_date)
				.MapVarchar("identifier_link", x => x.identifier_link)
				.MapTimeStampTz("date_of_data", x => x.date_of_data)
				.MapInteger("record_status_id", x => x.record_status_id);


		public static PostgreSQLCopyHelper<StudyTitle> study_titles_helper =
			new PostgreSQLCopyHelper<StudyTitle>("st", "study_titles")
				.MapInteger("study_id", x => x.study_id)
				.MapVarchar("title_text", x => x.title_text)
				.MapInteger("title_type_id", x => x.title_type_id)
                .MapVarchar("title_lang_code", x => x.title_lang_code)
				.MapInteger("lang_usage_id", x => x.lang_usage_id)
				.MapBoolean("is_default", x => x.is_default)
				.MapVarchar("comments", x => x.comments)
				.MapTimeStampTz("date_of_data", x => x.date_of_data)
				.MapInteger("record_status_id", x => x.record_status_id);


		public static PostgreSQLCopyHelper<StudyTopic> study_topics_helper =
			new PostgreSQLCopyHelper<StudyTopic>("st", "study_topics")
				.MapInteger("study_id", x => x.study_id)
				.MapInteger("topic_type_id", x => x.topic_type_id)
				.MapVarchar("topic_value", x => x.topic_value)
				.MapInteger("topic_ct_id", x => x.topic_ct_id)
				.MapVarchar("topic_ct_code", x => x.topic_ct_code)
				.MapVarchar("where_found", x => x.where_found)
				.MapTimeStampTz("date_of_data", x => x.date_of_data)
				.MapInteger("record_status_id", x => x.record_status_id);


		public static PostgreSQLCopyHelper<StudyContributor> study_contributors_helper =
			new PostgreSQLCopyHelper<StudyContributor>("st", "study_contributors")
				.MapInteger("study_id", x => x.study_id)
				.MapInteger("contrib_type_id", x => x.contrib_type_id)
				.MapBoolean("is_individual", x => x.is_individual)
				.MapInteger("organisation_id", x => x.organisation_id)
				.MapVarchar("organisation_name", x => x.organisation_name)
				.MapInteger("person_id", x => x.person_id)
				.MapVarchar("person_given_name", x => x.person_given_name)
				.MapVarchar("person_family_name", x => x.person_family_name)
				.MapVarchar("person_full_name", x => x.person_full_name)
				.MapVarchar("person_identifier", x => x.person_identifier)
				.MapVarchar("identifier_type", x => x.identifier_type)
				.MapVarchar("person_affiliation", x => x.person_affiliation)
				.MapVarchar("affil_org_id", x => x.affil_org_id)
		    	.MapVarchar("affil_org_id_type", x => x.affil_org_id_type)
				.MapTimeStampTz("date_of_data", x => x.date_of_data)
				.MapInteger("record_status_id", x => x.record_status_id);


		public static PostgreSQLCopyHelper<StudyRelationship> study_relationship_helper =
			new PostgreSQLCopyHelper<StudyRelationship>("st", "study_relationships")
				.MapInteger("study_id", x => x.study_id)
				.MapInteger("relationship_type_id", x => x.relationship_type_id)
				.MapVarchar("target_sd_id", x => x.target_sd_id)
			    .MapInteger("target_id", x => x.target_id)
				.MapTimeStampTz("date_of_data", x => x.date_of_data)
				.MapInteger("record_status_id", x => x.record_status_id);


		public static PostgreSQLCopyHelper<DataObject> data_objects_helper =
			new PostgreSQLCopyHelper<DataObject>("ob", "data_objects")
				.MapInteger("id", x => x.id)
				.MapVarchar("display_title", x => x.display_title)
			    .MapVarchar("doi", x => x.doi)
				.MapInteger("doi_status_id", x => x.doi_status_id)
				.MapInteger("publication_year ", x => x.publication_year)
				.MapInteger("object_class_id", x => x.object_class_id)
				.MapInteger("object_type_id", x => x.object_type_id)
				.MapInteger("managing_org_id", x => x.managing_org_id)
				.MapVarchar("managing_org", x => x.managing_org)
				.MapInteger("access_type_id", x => x.access_type_id)
				.MapVarchar("access_details", x => x.access_details)
				.MapVarchar("access_details_url", x => x.access_details_url)
				.MapDate("url_last_checked", x => x.url_last_checked)
				.MapBoolean("add_study_contribs", x => x.add_study_contribs)
				.MapBoolean("add_study_topics", x => x.add_study_topics)
				.MapTimeStampTz("date_of_data", x => x.date_of_data)
				.MapInteger("record_status_id", x => x.record_status_id);


		public static PostgreSQLCopyHelper<DataSetProperties> dataset_properties_helper =
			new PostgreSQLCopyHelper<DataSetProperties>("ob", "dataset_properties")
				.MapInteger("id", x => x.id)
				.MapInteger("record_keys_type_id", x => x.record_keys_type_id)
				.MapVarchar("record_keys_details", x => x.record_keys_details)
				.MapInteger("identifiers_type_id", x => x.identifiers_type_id)
				.MapVarchar("identifiers_details", x => x.identifiers_details)
				.MapInteger("consents_type_id", x => x.consents_type_id)
				.MapVarchar("consents_details", x => x.consents_details)
				.MapTimeStampTz("date_of_data", x => x.date_of_data)
				.MapInteger("record_status_id", x => x.record_status_id);


		public static PostgreSQLCopyHelper<DataObjectTitle> object_titles_helper =
			new PostgreSQLCopyHelper<DataObjectTitle>("ob", "object_titles")
				.MapInteger("object_id", x => x.object_id)
				.MapVarchar("title_text", x => x.title_text)
				.MapInteger("title_type_id", x => x.title_type_id)
				.MapVarchar("title_lang_code", x => x.title_lang_code)
				.MapInteger("lang_usage_id", x => x.lang_usage_id)
				.MapBoolean("is_default", x => x.is_default)
				.MapTimeStampTz("date_of_data", x => x.date_of_data)
				.MapInteger("record_status_id", x => x.record_status_id);


		public static PostgreSQLCopyHelper<DataObjectInstance> object_instances_helper =
			new PostgreSQLCopyHelper<DataObjectInstance>("ob", "object_instances")
				.MapInteger("object_id", x => x.object_id)
				.MapInteger("repository_org_id", x => x.repository_org_id)
				.MapVarchar("repository_org", x => x.repository_org)
				.MapVarchar("url", x => x.url)
				.MapBoolean("url_accessible", x => x.url_accessible)
				.MapDate("url_last_checked", x => x.url_last_checked)
				.MapInteger("resource_type_id", x => x.resource_type_id)
				.MapVarchar("resource_size", x => x.resource_size)
				.MapVarchar("resource_size_units", x => x.resource_size_units)
				.MapTimeStampTz("date_of_data", x => x.date_of_data)
				.MapInteger("record_status_id", x => x.record_status_id);


		public static PostgreSQLCopyHelper<DataObjectDate> object_dates_helper =
			new PostgreSQLCopyHelper<DataObjectDate>("ob", "object_dates")
				.MapInteger("object_id", x => x.object_id)
				.MapInteger("date_type_id", x => x.date_type_id)
				.MapVarchar("date_type", x => x.date_type)
				.MapInteger("start_year", x => x.start_year)
				.MapInteger("start_month", x => x.start_month)
				.MapInteger("start_day", x => x.start_day)
				.MapVarchar("date_as_string", x => x.date_as_string)
				.MapTimeStampTz("date_of_data", x => x.date_of_data)
				.MapInteger("record_status_id", x => x.record_status_id);
	}
}
