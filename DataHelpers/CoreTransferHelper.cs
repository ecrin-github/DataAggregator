using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class CoreDataTransferrer
    {
		DataLayer repo;
		string connString;
		DBUtilities db;

		public CoreDataTransferrer(DataLayer _repo)
		{
			repo = _repo;
			connString = repo.ConnString;
			db = new DBUtilities(connString);
		}

		public void LoadCoreStudyData()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"INSERT INTO core.studies(id, 
                        display_title, title_lang_code, brief_description, data_sharing_statement,
                        study_start_year, study_start_month, study_type_id, study_status_id,
                        study_enrolment, study_gender_elig_id, min_age, min_age_units_id,
                        max_age, max_age_units_id)
                        SELECT s.id,
                        s.display_title, s.title_lang_code, s.brief_description, s.data_sharing_statement,
                        s.study_start_year, s.study_start_month, s.study_type_id, s.study_status_id,
                        s.study_enrolment, s.study_gender_elig_id, s.min_age, s.min_age_units_id,
                        s.max_age, s.max_age_units_id
						FROM st.studies s ";

				int res = db.ExecuteCoreTransferSQL(sql_string, "study_relationships", 10001);
			}
		}


		public void LoadCoreStudyIdentifiers()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// Note may apply to new identifiers of existing studies as well as 
				// identifiers attached to new studies

				// action should depend on whether study id / source is the 'preferred ' for this study
				string sql_string = @"INSERT INTO core.study_identifiers(id, study_id, 
                        identifier_type_id, identifier_org_id, identifier_org, 
                        identifier_value, identifier_date, identifier_link)
                        SELECT t.study_id,
                        s.identifier_type_id, s.identifier_org_id, s.identifier_org, 
                        s.identifier_value, s.identifier_date, s.identifier_link
						FROM st.study_identifiers s ";

				int res = db.ExecuteCoreTransferSQL(sql_string, "study_relationships", 10001);
			}
		}


		public void LoadCoreStudyTitles()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// action should depend on whether study id / source is the 'preferred ' for this study
				string sql_string = @"INSERT INTO core.study_titles(id, study_id, 
                        title_type_id, title_text, lang_code, 
                        lang_usage_id, is_default, comments )
                        SELECT s.id, s.study_id,
                        s.title_type_id, s.title_text, s.lang_code, 
                        s.lang_usage_id, s.is_default, s.comments 
                        current_timestamp, s.record_status_id
						FROM st.study_titles s ";

				int res = db.ExecuteCoreTransferSQL(sql_string, "study_relationships", 10001);
			}
		}


		public void LoadCoreStudyContributors()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// action should depend on whether study id / source is the 'preferred ' for this study
				string sql_string = @"INSERT INTO core.study_contributors(id, study_id, 
                        contrib_type_id, is_individual, organisation_id, 
                        organisation_name, person_id, person_given_name,
                        person_family_name, person_full_name, person_identifier,
                        identifier_type, affil_org_id, affil_org_id_type )
                        SELECT s.id, s.study_id,
                        s.contrib_type_id, s.is_individual, s.organisation_id, 
                        s.organisation_name, s.person_id, s.person_given_name,
                        s.person_family_name, s.person_full_name, s.person_identifier,
                        s.identifier_type, s.affil_org_id, s.affil_org_id_type 
						FROM st.study_contributors s ";

				int res = db.ExecuteCoreTransferSQL(sql_string, "study_relationships", 10001);
			}
		}


		public void LoadCoreStudyTopics()
		{
            using (var conn = new NpgsqlConnection(connString))
            {
                // action should depend on whether study id / source is the 'preferred ' for this study
                string sql_string = @"INSERT INTO core.study_topics(id, study_id, 
                        topic_type_id, mesh_coded, topic_code, 
                        topic_value, topic_qualcode, topic_qualvalue,
                        original_ct_id, original_ct_code, original_value, comments)
                        SELECT s.id, s.study_id,
                        s.topic_type_id, s.mesh_coded, s.topic_code, 
                        s.topic_value, s.topic_qualcode, s.topic_qualvalue,
                        s.original_ct_id, s.original_ct_code, s.original_value, s.comments
						FROM st.study_topics s ";

				int res = db.ExecuteCoreTransferSQL(sql_string, "study_relationships", 10001);
			}
        }

		public void LoadCoreStudyFeatures()
		{
            using (var conn = new NpgsqlConnection(connString))
            {
                // action should depend on whether study id / source is the 'preferred ' for this study
                string sql_string = @"INSERT INTO core.study_features(id, study_id, 
                        feature_type_id, feature_value_id )
                        SELECT s.id, s.study_id,
                        s.feature_type_id, s.feature_value_id 
						FROM st.study_features s ";

				int res = db.ExecuteCoreTransferSQL(sql_string, "study_relationships", 10001);
			}
        }


		public void LoadCoreStudyRelationShips()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// action should depend on whether study id / source is the 'preferred ' for this study

				string sql_string = @"INSERT INTO core.study_relationships(id, study_id, 
                        relationship_type_id, target_study_id)
                        SELECT s.id, s.study_id,
                        s.relationship_type_id, target_study_id
						FROM st.study_relationships s ";

				int res = db.ExecuteCoreTransferSQL(sql_string, "study_relationships", 10001);
			}
		}


		public void LoadCoreDataObjects()
		{
			string sql_string = @"INSERT INTO core.data_objects(id, study_id,
                    display_title, version, doi, doi_status_id, publication_year,
                    object_class_id, object_type_id, managing_org_id, managing_org,
                    lang_code, access_type_id, access_details, access_details_url,
                    url_last_checked, eosc_category, add_study_contribs, 
                    add_study_topics)
                    SELECT s.id, s.study_id,
                    s.display_title, s.version, s.doi, s.doi_status_id, s.publication_year,
                    s.object_class_id, s.object_type_id, s.managing_org_id, s.managing_org,
                    s.lang_code, s.access_type_id, s.access_details, s.access_details_url,
                    s.url_last_checked, s.eosc_category, s.add_study_contribs, 
                    s.add_study_topics
                    FROM ob.data_objects s ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "data_objects", 10001);

		}


		public void LoadCoreObjectDatasets()
		{
			string sql_string = @"INSERT INTO core.object_datasets(id, object_id, 
            record_keys_type_id, record_keys_details, 
            deident_type_id, deident_direct, deident_hipaa,
            deident_dates, deident_nonarr, deident_kanon, deident_details,
			consent_type_id, consent_noncommercial, consent_geog_restrict,
			consent_research_type, consent_genetic_only, consent_no_methods, consent_details)
            SELECT s.id, s.object_id, 
            record_keys_type_id, record_keys_details, 
            deident_type_id, deident_direct, deident_hipaa,
            deident_dates, deident_nonarr, deident_kanon, deident_details,
			consent_type_id, consent_noncommercial, consent_geog_restrict,
			consent_research_type, consent_genetic_only, consent_no_methods, consent_details
			FROM ob.object_datasets s ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "object_datasets", 10001);
		}


		public void LoadCoreObjectInstances()
		{
			string sql_string = @"INSERT INTO core.object_instances(id, object_id,  
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, resource_comments)
            SELECT s.id, s.object_id, 
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, resource_comments
			FROM ob.object_instances s ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "object_instances", 10001);
		}


		public void LoadCoreObjectTitles()
		{
			string sql_string = @"INSERT INTO core.object_titles(id, object_id, 
            title_type_id, title_text, lang_code,
            lang_usage_id, is_default, comments)
            SELECT s.id, s.object_id, 
            title_type_id, title_text, lang_code,
            lang_usage_id, is_default, comments
			FROM ob.object_titles s ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "object_titles", 10001);
		}


		public void LoadCoreObjectDates()
		{
			string sql_string = @"INSERT INTO core.object_dates(id, object_id,  
            date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details)
            SELECT s.id, s.object_id, 
            date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details
			FROM ob.object_dates s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "object_dates", 10001);
		}


		public void LoadCoreObjectContributors()
		{
			string sql_string = @"INSERT INTO core.object_contributors(id, object_id, 
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type)
            SELECT s.id, s.object_id, 
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type
			FROM ob.object_contributors s ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "object_contributors", 10001);
		}


		public void LoadCoreObjectTopics()
		{
			string sql_string = @"INSERT INTO core.object_topics(id, object_id,  
            topic_type_id, mesh_coded, topic_code, topic_value, 
            topic_qualcode, topic_qualvalue, original_ct_id, original_ct_code,
            original_value, comments)
            SELECT s.id, s.object_id, 
            topic_type_id, mesh_coded, topic_code, topic_value, 
            topic_qualcode, topic_qualvalue, original_ct_id, original_ct_code,
            original_value, comments
			FROM ob.object_topics s ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "object_topics", 10001);
		}


		public void LoadCoreObjectDescriptions()
		{
			string sql_string = @"INSERT INTO core.object_descriptions(id, object_id, 
            description_type_id, label, description_text, lang_code, 
            contains_html)
            SELECT s.id, s.object_id, 
            description_type_id, label, description_text, lang_code, 
            contains_html
			FROM ob.object_descriptions s ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "object_descriptions", 10001);
		}


		public void LoadCoreObjectIdentifiers()
		{
			string sql_string = @"INSERT INTO core.object_identifiers(id, object_id, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date)
            SELECT s.id, s.object_id, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date
			FROM ob.object_identifiers s ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "object_identifiers", 10001);
		}


		public void LoadCoreObjectRelationships()
		{
			string sql_string = @"INSERT INTO core.object_relationships(id, object_id,   
            relationship_type_id, target_object_id)
            SELECT s.id, s.object_id, 
            s.relationship_type_id, s.target_object_id
			FROM ob.object_relationships s ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "object_relationships", 10001);
		}


		public void LoadCoreObjectRights()
		{
			string sql_string = @"INSERT INTO core.object_rights(id, object_id,  
            rights_name, rights_uri, comments)
            SELECT s.id, s.object_id,
            rights_name, rights_uri, comments
			FROM ob.object_rights s ";

			int res = db.ExecuteCoreTransferSQL(sql_string, "object_rights", 10001);
		}
	}
}
