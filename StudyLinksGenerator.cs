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
				string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_links_collector;
                        CREATE TABLE nk.temp_study_links_collector(
				        source_id int
                      , sd_id varchar
                      , preferred_sd_id varchar
                      , preferred_source_id int) ";
				conn.Execute(sql_string);
			}
		}

		public void TransferLinksToCollectorTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				// needs to be done twice to keep the ordering of aouerces correct

				string sql_string = @"INSERT INTO nk.temp_study_links_collector(
				          source_id, sd_id, preferred_sd_id, preferred_source_id) 
                          SELECT t.source_1, t.sd_id_1, t.sd_id_2, t.source_2
						  FROM nk.temp_study_links_by_source t
                          inner join nk.source_preference_ratings r1
                          on t.source_1 = r1.source_id
                          inner join nk.source_preference_ratings r2
                          on t.source_2 = r2.source_id
                          WHERE r1.rating > r2.rating";

				conn.Execute(sql_string);

				sql_string = @"INSERT INTO nk.temp_study_links_collector(
				          source_id, sd_id, preferred_sd_id, preferred_source_id) 
                          SELECT t.source_2, t.sd_id_2, t.sd_id_1, t.source_1
						  FROM nk.temp_study_links_by_source t
                          inner join nk.source_preference_ratings r1
                          on t.source_1 = r1.source_id
                          inner join nk.source_preference_ratings r2
                          on t.source_2 = r2.source_id
                          WHERE r1.rating < r2.rating";

				conn.Execute(sql_string);
			}
		}


		public void MakeLinksDistinct()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				// needs to be done twice to keep the ordering of aouerces correct

				string sql_string = @"DROP TABLE IF EXISTS nk.distinct_links;
                           CREATE TABLE nk.distinct_links 
                           as SELECT distinct source_id, sd_id, preferred_sd_id, preferred_source_id
						   FROM nk.temp_study_links_collector";

				conn.Execute(sql_string);
			}
		}


		public void CascadeLinksTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				// needs to be done twice to keep the ordering of aouerces correct

				int match_number = 500;  // arbitrary start number
				while (match_number > 0)
				{
					// get match number as numbver of link records where the rhs sd_id
					// appears elsewhere on the left...

					string sql_string = @"SELECT count(*) 
						  FROM nk.distinct_links t1
                          inner join nk.distinct_links t2
                          on t1.preferred_source_id = t2.source_id
                          and t1.preferred_sd_id = t2.sd_id";

					match_number = conn.ExecuteScalar<int>(sql_string);

					if (match_number > 0)
					{
						// do the update
						sql_string = @"UPDATE nk.distinct_links t1
                          SET preferred_source_id = t2.preferred_source_id,
                          preferred_sd_id = t2.preferred_sd_id
						  FROM nk.distinct_links t2
                          WHERE t1.preferred_source_id = t2.source_id
                          AND t1.preferred_sd_id = t2.sd_id";

						conn.Execute(sql_string);
					}
				}
			}
		}


		public void FindMultipleRelationships()
		{


		}

		public void TransferNewLinksToDataTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = @"DROP TABLE IF EXISTS nk.study_study_links;
                          CREATE TABLE nk.study_study_links 
                          as TABLE nk.distinct_links";

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


		public void DropTempTables()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_study_links_by_source";
				conn.Execute(sql_string);

				sql_string = "DROP TABLE IF EXISTS nk.temp_study_links_collector";
				conn.Execute(sql_string);

				sql_string = "DROP TABLE IF EXISTS nk.distinct_links";
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
