using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class StudyLinksGenerator
    {
		DataLayer repo;
		string mdr_connString;

		public StudyLinksGenerator(DataLayer _repo)
		{
			repo = _repo;
			mdr_connString = repo.GetMDRConnString();
		}

		public IEnumerable<StudyLink> FetchLinks(int org_id)
		{
			string conn_string = repo.GetConnString(org_id);
			using (var conn = new NpgsqlConnection(conn_string))
			{
				string sql_string;
				if (org_id != 100115)
				{
					sql_string = @"select " + org_id.ToString() + @" as source_1, sd_id as sd_id_1, 
                          identifier_value as sd_id_2, identifier_org_id as source_2
                          from ad.study_identifiers
                          where identifier_type_id = 11
                          and identifier_org_id <> 100115
                          and identifier_org_id <> " + org_id.ToString();
				}
				else
				{
					sql_string = @"select source_1, sd_id_1, sd_id_2, source_2
                          from ad.temp_who_ids;";
				}
				return conn.Query<StudyLink>(sql_string);
			}
		}


		public void SetUpTempLinksBySourceTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = @"CREATE TABLE IF NOT EXISTS nk.temp_study_links_by_source(
				        source_1 int
                      , sd_id_1 varchar
                      , sd_id_2 varchar
                      , source_2 int) ";
				conn.Execute(sql_string);
			}
		}

		public void TruncateLinksBySourceTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = @"TRUNCATE TABLE nk.temp_study_links_by_source";
				conn.Execute(sql_string);
			}
		}

		public void SetUpTempLinkCollectorTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = @"CREATE TABLE IF NOT EXISTS nk.temp_study_links_collector(
				        source_1 int
                      , sd_id_1 varchar
                      , sd_id_2 varchar
                      , source_2 int) ";
				conn.Execute(sql_string);
			}
		}

		public void TransferLinksToCollectorTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				// needs to be done twice to keep the ordering of aouerces correct

				string sql_string = @"INSERT INTO nk.temp_study_links_collector(
				          source_1, sd_id_1, sd_id_2, source_2) 
                          SELECT source_1, sd_id_1, sd_id_2, source_2
						  FROM nk.temp_study_links_by_source t
                          WHERE source_1 > source_2";

				conn.Execute(sql_string);

				sql_string = @"INSERT INTO nk.temp_study_links_collector(
				          source_1, sd_id_1, sd_id_2, source_2) 
                          SELECT source_2, sd_id_2, sd_id_1, source_1
						  FROM nk.temp_study_links_by_source t
                          WHERE source_1 < source_2";

				conn.Execute(sql_string);
			}
		}


		public void TransferNewLinksToDataTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = @"TRUNCATE TABLE nk.study_study_links;
                          INSERT INTO nk.study_study_links(
				          studya_source_id, studya_sd_id, studyb_sd_id, studyb_source_id) 
                          SELECT distinct source_1, sd_id_1, sd_id_2, source_2
						  FROM nk.temp_study_links_collector";

				conn.Execute(sql_string);
			}
		}


		public int ObtainTotalOfNewLinks()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = @"SELECT COUNT(*) FROM nk.temp_study_links_collector";
				return conn.ExecuteScalar<int>(sql_string);
			}
		}


		public void DropTempLinksBySourceTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_study_links_by_source";
				conn.Execute(sql_string);
			}
		}

		public void DropTempLinkCollectorTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_study_links_collector";
				conn.Execute(sql_string);
			}
		}

		public ulong StoreLinks(PostgreSQLCopyHelper<StudyLink> copyHelper, IEnumerable<StudyLink> entities)
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		// Returns the total number of PubMed Ids to be processd

		public int GetSourceRecordCount()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string query_string = @"SELECT COUNT(*) FROM sf.source_data_objects 
                                WHERE source_id = 100135 AND download_status = 0";
				return conn.ExecuteScalar<int>(query_string);
			}
		}

	}
}
