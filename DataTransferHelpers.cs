using Dapper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
	class NewStudiesTransferrer
	{
		string db_conn;

		public NewStudiesTransferrer(string _db_conn)
		{
			db_conn = _db_conn;
		}


		public void TransferStudies()
		{
			string sql_string = @"INSERT INTO ad.studies (sd_id, hash_id, display_title,
            title_lang_code, brief_description, bd_contains_html, data_sharing_statement,
            dss_contains_html, study_start_year, study_start_month, study_type_id, 
            study_status_id, study_enrolment, study_gender_elig_id, min_age, 
            min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
            record_hash, study_full_hash)
            SELECT s.sd_id, s.hash_id, display_title,
            title_lang_code, brief_description, bd_contains_html, data_sharing_statement,
            dss_contains_html, study_start_year, study_start_month, study_type_id, 
            study_status_id, study_enrolment, study_gender_elig_id, min_age, 
            min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
            record_hash, study_full_hash 
            FROM sd.studies s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void UpdateWithADIDs()
		{
			string sql_string = @"UPDATE ad.temp_new_studies ns
            SET study_ad_id = a.ad_id
            FROM ad.studies a
            WHERE a.sd_id = ns.sd_id;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferStudyIdentifiers()
		{
			string sql_string = @"INSERT INTO ad.study_identifiers(study_ad_id, study_hash_id, sd_id,
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash
            FROM sd.study_identifiers s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferStudyTitles()
		{
			string sql_string = @"INSERT INTO ad.study_titles(study_ad_id, study_hash_id, sd_id,
            title_text, title_type_id, title_lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            title_text, title_type_id, title_lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash
            FROM sd.study_titles s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id"; 

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferStudyReferences()
		{
			string sql_string = @"INSERT INTO ad.study_references(study_ad_id, study_hash_id, sd_id,
            pmid, citation, doi, comments, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            pmid, citation, doi, comments, record_hash
            FROM sd.study_references s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferStudyContributors()
		{
			string sql_string = @"INSERT INTO ad.study_contributors(study_ad_id, study_hash_id, sd_id,
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type, record_hash
            FROM sd.study_contributors s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferStudyTopics()
		{
			string sql_string = @"INSERT INTO ad.study_topics(study_ad_id, study_hash_id, sd_id,
            topic_type_id, topic_value, topic_ct_id, topic_ct_code,
            where_found, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            topic_type_id, topic_value, topic_ct_id, topic_ct_code,
            where_found, record_hash
            FROM sd.study_topics s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferStudyRelationships()
		{
			string sql_string = @"INSERT INTO ad.study_relationships(study_ad_id, study_hash_id, sd_id,
            relationship_type_id, target_sd_id, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            relationship_type_id, target_sd_id, record_hash
            FROM sd.study_relationships s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}

			// needs the target ad_id to be updated too...
			sql_string = @"Update ad.study_relationships r
            SET target_ad_id = a.ad_id
            FROM ad.studies a
            WHERE r.target_sd_id = a.sd_id;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferStudyFeatures()
		{
			string sql_string = @"INSERT INTO ;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferStudyHashes()
		{
			string sql_string = @"INSERT INTO ad.study_hashes(study_ad_id, study_hash_id, sd_id,
			hash_type_id, composite_hash)
			SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            hash_type_id, composite_hash
			FROM sd.study_hashes s
			INNER JOIN ad.temp_new_studies ns
			ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}


	class NewDataObjectsTransferrer
	{
		string db_conn;

		public NewDataObjectsTransferrer(string _db_conn)
		{
			db_conn = _db_conn;
		}


		public void TransferDataObjects()
		{
			string sql_string = @"INSERT INTO ad.data_objects(study_ad_id, object_hash_id, 
            study_hash_id, sd_id, do_id, display_title, doi, doi_status_id, publication_year,
            object_class_id, object_type_id, managing_org_id, managing_org, access_type_id,
            access_details, access_details_url, url_last_checked, add_study_contribs,
            add_study_topics, datetime_of_data_fetch, record_hash, object_full_hash)
            SELECT s.ad_id, nd.object_hash_id, 
            study_hash_id, nd.sd_id, nd.do_id, d.display_title, doi, doi_status_id, publication_year,
            object_class_id, object_type_id, managing_org_id, managing_org, access_type_id,
            access_details, access_details_url, url_last_checked, add_study_contribs,
            add_study_topics, d.datetime_of_data_fetch, d.record_hash, object_full_hash
            FROM sd.data_objects d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id
            INNER JOIN ad.studies s
            ON d.sd_id = s.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void UpdateWithADIDs()
		{
			string sql_string = @"UPDATE ad.temp_new_data_objects nd
            SET object_ad_id = d.ad_id
            FROM ad.data_objects d
            WHERE nd.sd_id = d.sd_id
            AND nd.do_id = d.do_id;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferDataSetProperties()
		{
			string sql_string = @"INSERT INTO ad.dataset_properties(object_ad_id, object_hash_id, 
            sd_id, do_id, record_keys_type_id, record_keys_details, identifiers_type_id,
            identifiers_details, consents_type_id, consents_details, record_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            nd.sd_id, nd.do_id, record_keys_type_id, record_keys_details, identifiers_type_id,
            identifiers_details, consents_type_id, consents_details, record_hash
            FROM sd.dataset_properties d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferObjectInstances()
		{
			string sql_string = @"INSERT INTO ad.object_instances(object_ad_id, object_hash_id, 
            sd_id, do_id, instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, record_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            nd.sd_id, nd.do_id, instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, record_hash
            FROM sd.object_instances d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferObjectTitles()
		{
			string sql_string = @"INSERT INTO ad.object_titles(object_ad_id, object_hash_id, 
            sd_id, do_id, title_text, title_type_id, title_lang_code,
            lang_usage_id, is_default, comments, comparison_text, record_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            nd.sd_id, nd.do_id, title_text, title_type_id, title_lang_code,
            lang_usage_id, is_default, comments, comparison_text, record_hash
            FROM sd.object_titles d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferObjectLanguages()
		{
			string sql_string = @"INSERT INTO ad.object_languages(object_ad_id, object_hash_id, 
            sd_id, do_id, lang_code, record_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            nd.sd_id, nd.do_id, lang_code, record_hash
            FROM sd.object_languages d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferObjectDates()
		{
			string sql_string = @"INSERT INTO ad.object_dates(object_ad_id, object_hash_id, 
            sd_id, do_id, date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details, record_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            nd.sd_id, nd.do_id, date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details, record_hash
            FROM sd.object_dates d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferObjectHashes()
		{
			string sql_string = @"INSERT INTO ad.object_hashes(object_ad_id, object_hash_id, 
            study_hash_id, sd_id, do_id, hash_type_id, composite_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            study_hash_id, nd.sd_id, nd.do_id, hash_type_id, composite_hash
            FROM sd.object_hashes d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

	}

}
