using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
	public class ObjectDataTransferrer
    {

		DataLayer repo;
		string connString;
		DBUtilities db;

		public ObjectDataTransferrer(DataLayer _repo)
		{
			repo = _repo;
			connString = repo.ConnString;
			db = new DBUtilities(connString);
		}

		public void SetUpTempObjectIdsTables()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
                      CREATE TABLE IF NOT EXISTS nk.temp_object_ids(
				        object_id                INT
                      , source_id                INT
                      , sd_oid                   VARCHAR
                      , parent_study_sd_sid      VARCHAR
                      , parent_study_id          INT
                      , is_preferred_study       BOOLEAN
                      , is_preferred_object      BOOLEAN         NOT NULL DEFAULT true
                      , use_this_link            BOOLEAN         NOT NULL DEFAULT true
                      , datetime_of_data_fetch   TIMESTAMPTZ
                      ); ";
				conn.Execute(sql_string);

				sql_string = @"DROP TABLE IF EXISTS nk.temp_objects_to_add;
                      CREATE TABLE IF NOT EXISTS nk.temp_objects_to_add(
				        object_id                INT
                      , sd_oid                   VARCHAR
                      ); 
                      CREATE INDEX temp_objects_to_add_sd_oid ON nk.temp_objects_to_add(sd_oid);";
				conn.Execute(sql_string);
			}
		}


		public IEnumerable<ObjectIds> FetchObjectIds(int source_id)
		{
			string conn_string = repo.GetConnString(source_id);
			using (var conn = new NpgsqlConnection(conn_string))
			{
				string sql_string = @"select " + source_id.ToString() + @" as source_id, 
                          sd_oid, sd_sid as parent_study_sd_sid, datetime_of_data_fetch
                          from ad.data_objects";

				return conn.Query<ObjectIds>(sql_string);
			}
		}


		public ulong StoreObjectIds(PostgreSQLCopyHelper<ObjectIds> copyHelper, IEnumerable<ObjectIds> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public void UpdateObjectStudyIds(int source_id)
        {
			// Update the object parent study_id using the 'correct'
			// value found in the all_ids_studies table

			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"UPDATE nk.temp_object_ids t
		                   SET parent_study_id = s.study_id, 
                               is_preferred_study = s.is_preferred
                           FROM nk.all_ids_studies s
                           WHERE t.parent_study_sd_sid = s.sd_sid
                           and s.source_id = " + source_id.ToString();
				conn.Execute(sql_string);
			}
		}


		public void CheckStudyObjectsForDuplicates(int source_id)
        {
			// TO DO - very rare at the momentt
        }


		public void UpdateAllObjectIdsTable(int source_id)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// Add the new object id records to the all Ids table
				// The assumption here is that within each source the data object sd_oid is unique,
				// (because they are each linked to different studies) which means 
				// that the link is also unique.

				string sql_string = @"INSERT INTO nk.all_ids_objects
                             (source_id, sd_oid, parent_study_sd_sid,
                             parent_study_id, is_preferred_study, use_this_link, datetime_of_data_fetch)
                             select source_id, sd_oid, parent_study_sd_sid,
                             parent_study_id, is_preferred_study, use_this_link, datetime_of_data_fetch
                             from nk.temp_object_ids";
				conn.Execute(sql_string);

				// update the table with the object id (will always be the same as the 
				// identity at the moment as there is no object-object checking
				// If objects are amalgamated from different sources in the future
				// the object-object check will need to be added at this stage

				sql_string = @"UPDATE nk.all_ids_objects
                            SET object_id = id
                            WHERE source_id = " + source_id.ToString() + @"
							and object_id is null;";
				conn.Execute(sql_string);

				// Update the temporary table to ensure that the object id is included
				/*
				sql_string = @"UPDATE nk.temp_object_ids t
                             SET object_id = s.object_id 
                             FROM nk.all_ids_objects s
                             WHERE t.sd_oid = s.sd_oid
                             AND source_id = " + source_id.ToString();
				conn.Execute(sql_string);
				*/
			}
		}


        public void FillObjectsToAddTable(int source_id)
        {
            using (var conn = new NpgsqlConnection(connString))
            {
                string sql_string = @"INSERT INTO nk.temp_objects_to_add
                             (object_id, sd_oid)
                             SELECT distinct object_id, sd_oid 
                             FROM nk.all_ids_objects
                             WHERE source_id = " + source_id.ToString(); 
                conn.Execute(sql_string);
            }
        }


		public IEnumerable<ObjectIds> FetchPMIDs(int source_id)
		{
			string conn_string = repo.GetConnString(source_id);
			using (var conn = new NpgsqlConnection(conn_string))
			{
				string sql_string = @"select k.source_id, 
                        k.pmid as sd_oid, k.sd_sid as parent_study_sd_sid, a.datetime_of_data_fetch
                        from pp.total_pubmed_links k
                        inner join ad.data_objects a
                        on k.pmid = a.sd_oid";
				return conn.Query<ObjectIds>(sql_string);
			}
		}

		public void InputPreferredSDSIDS()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// replace any LHS ad_sids with the 'preferred' RHS

				string sql_string = @"UPDATE nk.temp_object_ids b
                               SET parent_study_sd_sid = preferred_sd_sid
                               FROM nk.study_study_links k
                               WHERE b.parent_study_sd_sid = k.sd_sid;";
				conn.Execute(sql_string);

				// That may have produced some duplicates - if so get rid of them
				sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids2;
                           CREATE TABLE nk.temp_object_ids2 
                           as SELECT distinct * FROM nk.temp_object_ids";
				conn.Execute(sql_string);

				sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
				ALTER TABLE nk.temp_object_ids2 RENAME TO temp_object_ids;";
				conn.Execute(sql_string);
			}
		}


		public void AddPMIDLinksToAllObjectIdsTable ()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"INSERT INTO nk.all_ids_objects
                             (source_id, sd_oid, parent_study_sd_sid,
                             parent_study_id, is_preferred_study, use_this_link, datetime_of_data_fetch)
                             select source_id, sd_oid, parent_study_sd_sid,
                             parent_study_id, is_preferred_study, use_this_link, datetime_of_data_fetch
                             from nk.temp_object_ids";
				conn.Execute(sql_string);
			}
		}


		public void ResetIdsOfDuplicatedPMIDs(int source_id)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// Find the minimum object_id for each PMID in the table

				string sql_string = @"CREATE TABLE temp_min_object_ids as
                                     SELECT sd_oid, Min(id) as min_id
                                     FROM nk.all_ids_objects
								     WHERE source_id = " + source_id.ToString() + @"
                                     GROUP BY sd_oid;";
				conn.Execute(sql_string);

				sql_string = @"UPDATE nk.all_ids_objects b
                               SET object_id = min_id
                               FROM temp_min_object_ids m
                               ON b.sd_oid = m.sd_oid
                               WHERE source_id = " + source_id.ToString();
				conn.Execute(sql_string);

				sql_string = @"DROP TABLE temp_min_object_ids;";
				conn.Execute(sql_string);
			}
		}


		public void LoadDataObjects(string schema_name)
		{
			 string sql_string = @"INSERT INTO ob.data_objects(id, study_id
                    display_title, version, doi, doi_status_id, publication_year,
                    object_class_id, object_type_id, managing_org_id, managing_org,
                    lang_code, access_type_id, access_details, access_details_url,
                    url_last_checked, eosc_category, add_study_contribs, 
                    add_study_topics)
                    SELECT t.object_id, t.study_id,
                    s.display_title, s.version, s.doi, s.doi_status_id, s.publication_year,
                    s.object_class_id, s.object_type_id, s.managing_org_id, s.managing_org,
                    s.lang_code, s.access_type_id, s.access_details, s.access_details_url,
                    s.url_last_checked, s.eosc_category, s.add_study_contribs, 
                    s.add_study_topics
                    FROM " + schema_name + @".data_objects s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			 db.ExecuteTransferSQL(sql_string, schema_name, "data_objects", "");

			 db.Update_SourceTable_ExportDate(schema_name, "data_objects");
		}

	
		public void LoadObjectDatasets(string schema_name)
		{
			string sql_string = @"INSERT INTO ob.object_datasets(object_id, 
            record_keys_type_id, record_keys_details, 
            deident_type_id, deident_direct, deident_hipaa,
            deident_dates, deident_nonarr, deident_kanon, deident_details,
			consent_type_id, consent_noncommercial, consent_geog_restrict,
			consent_research_type, consent_genetic_only, consent_no_methods, consent_details)
            SELECT t.object_id, 
            record_keys_type_id, record_keys_details, 
            deident_type_id, deident_direct, deident_hipaa,
            deident_dates, deident_nonarr, deident_kanon, deident_details,
			consent_type_id, consent_noncommercial, consent_geog_restrict,
			consent_research_type, consent_genetic_only, consent_no_methods, consent_details
			FROM " + schema_name + @".object_datasets s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			db.ExecuteTransferSQL(sql_string, schema_name, "object_datasets", "");

			db.Update_SourceTable_ExportDate(schema_name, "object_datasets");
		}


		public void LoadObjectInstances(string schema_name)
		{
			string sql_string = @"INSERT INTO ob.object_instances(object_id,  
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, resource_comments)
            SELECT t.object_id,
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, resource_comments
			FROM " + schema_name + @".object_instances s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			db.ExecuteTransferSQL(sql_string, schema_name, "object_instances", "");

			db.Update_SourceTable_ExportDate(schema_name, "object_instances");
		}


		public void LoadObjectTitles(string schema_name)
		{
			string sql_string = @"INSERT INTO ob.object_titles(object_id, 
            title_type_id, title_text, lang_code,
            lang_usage_id, is_default, comments)
            SELECT t.object_id, 
            title_type_id, title_text, lang_code,
            lang_usage_id, is_default, comments
			FROM " + schema_name + @".object_titles s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			db.ExecuteTransferSQL(sql_string, schema_name, "object_titles", "non-preferred");

			db.Update_SourceTable_ExportDate(schema_name, "object_titles");
		}


		public void LoadObjectDates(string schema_name)
		{
			string sql_string = @"INSERT INTO ob.object_dates(object_id, 
            date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details)
            SELECT t.object_id,
            date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details
			FROM " + schema_name + @".object_dates s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			db.ExecuteTransferSQL(sql_string, schema_name, "object_dates", "");

			db.Update_SourceTable_ExportDate(schema_name, "object_dates");
    	}


		public void LoadObjectContributors(string schema_name)
		{
			string sql_string = @"INSERT INTO ob.object_contributors(object_id, 
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type)
            SELECT t.object_id,
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type
			FROM " + schema_name + @".object_contributors s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			db.ExecuteTransferSQL(sql_string, schema_name, "object_contributors", "");

			db.Update_SourceTable_ExportDate(schema_name, "object_contributors");
		}



		public void LoadObjectTopics(string schema_name)
		{
			string sql_string = @"INSERT INTO ob.object_topics(object_id, 
            topic_type_id, mesh_coded, topic_code, topic_value, 
            topic_qualcode, topic_qualvalue, original_ct_id, original_ct_code,
            original_value, comments)
            SELECT t.object_id, 
            topic_type_id, mesh_coded, topic_code, topic_value, 
            topic_qualcode, topic_qualvalue, original_ct_id, original_ct_code,
            original_value, comments
			FROM " + schema_name + @".object_topics s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			db.ExecuteTransferSQL(sql_string, schema_name, "object_topics", "");

			db.Update_SourceTable_ExportDate(schema_name, "object_topics");
		}


		public void LoadObjectDescriptions(string schema_name)
		{
			string sql_string = @"INSERT INTO ob.object_descriptions(object_id, 
            description_type_id, label, description_text, lang_code, 
            contains_html)
            SELECT t.object_id,
            description_type_id, label, description_text, lang_code, 
            contains_html
			FROM " + schema_name + @".object_descriptions s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			db.ExecuteTransferSQL(sql_string, schema_name, "object_descriptions", "");

			db.Update_SourceTable_ExportDate(schema_name, "object_descriptions");
		}



		public void LoadObjectIdentifiers(string schema_name)
		{
			string sql_string = @"INSERT INTO ob.object_identifiers(object_id,  
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date)
            SELECT t.object_id,
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date
			FROM " + schema_name + @".object_identifiers s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			db.ExecuteTransferSQL(sql_string, schema_name, "object_identifiers", "");

			db.Update_SourceTable_ExportDate(schema_name, "object_identifiers");
		}



		public void LoadObjectRelationships(string schema_name)
		{
			string sql_string = @"INSERT INTO ob.object_relationships(object_id,  
            relationship_type_id)
            SELECT t.object_id,
            relationship_type_id
			FROM " + schema_name + @".object_relationships s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			// NEED TO DO UPDATE OF TARGET SEPARATELY

			db.ExecuteTransferSQL(sql_string, schema_name, "object_relationships", "");

			db.Update_SourceTable_ExportDate(schema_name, "object_relationships");
		}



		public void LoadObjectRights(string schema_name)
		{
			string sql_string = @"INSERT INTO ob.object_rights(object_id,  
            rights_name, rights_uri, comments)
            SELECT t.object_id,
            rights_name, rights_uri, comments
			FROM " + schema_name + @".object_rights s
					INNER JOIN nk.temp_objects_to_add t
                    on s.sd_oid = t.sd_oid ";

			db.ExecuteTransferSQL(sql_string, schema_name, "object_rights", "");

			db.Update_SourceTable_ExportDate(schema_name, "object_rights");
		}


		public void DropTempObjectIdsTable()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"DROP TABLE IF EXISTS nk.temp_object_ids;
					      DROP TABLE IF EXISTS nk.temp_objects_to_add;";
				conn.Execute(sql_string);
			}
		}

	}
}
