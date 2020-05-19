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
                          and identifier_org_id < 100133 
                          and identifier_org_id <> " + org_id.ToString();
				}
				else
				{
					sql_string = @"select source_id as source_1, sd_id as sd_id_1, 
                          identifier_value as sd_id_2, identifier_org_id as source_2
                          from ad.study_identifiers
                          where identifier_type_id = 11
                          and identifier_org_id <> 100115
                          and identifier_org_id < 100133 
                          and identifier_org_id <> source_id;";
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
				// telescope the preferred links to the most preferred
				// do as long as there remains links to be telescoped
				// (a few have to be done twice)
				string sql_string;
				int match_number = 500;  // arbitrary start number
				while (match_number > 0)
				{
					// get match number as numbver of link records where the rhs sd_id
					// appears elsewhere on the left...

					  sql_string = @"SELECT count(*) 
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

				// but in some cases the telescoped link may have already been 
				// present - i.e. a study has two other reg identifiers
				// one to the most preferred
				// Process above will result in duplicates in these cases
				// and these duplicates therefore need to be removed.
				sql_string = @"DROP TABLE IF EXISTS nk.distinct_links2;
                           CREATE TABLE nk.distinct_links2 
                           as SELECT distinct * FROM nk.distinct_links";

				conn.Execute(sql_string);
			}
		}


		// one set of relationships are not 'same study in a different registry'
		// but multiople studies in a different registry

		// such studies have a study relationship rather than being straight equivalents...
		// there can be multiple studies in the 'preferred' registry
		// or in the non-preferred registry - each group being equivalent to
		// a registry entrey that represents a single study, or sometimes a 
		// single project / programme, or grant

		// The study relationships are 
		// (source) 26: Is a member of a group of 2 or more studies registered elsewhere as a single study, (the referenced study).
		// (source) 25: Includes, as a member of a related group registered individually elsewhere, the referenced study.

		public void ManageLinkedPreferredSources()
		{
			string sql_string;

			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				sql_string = @"DROP TABLE IF EXISTS nk.linked_study_groups;
                     CREATE TABLE nk.linked_study_groups(
                           source_id int
                         , source_sd_id varchar
                         , relationship_id  int
						 , target_sd_id varchar
						 , target_source_id  int
                     );";

				conn.Execute(sql_string);

				// create a table that (working from the inside out)
				// a) gets the source id/sd_ids of thte LHS of the study links table
				//    that has more than one 'preferred' study associated with it (dataset d)
				// b) takes those records and identifies the distinct source ids
				//    that are linked to each LHS study (dataset a)
				// c) Further identifies the records that have just a single
				//    source referenced on the RHS (so all the linked records belong
				//    to the same registry (dataset agg)
				// d) joins that dataset back to the linked records table, to 
				//    identify the source records that meet the criteria of 
				//    having one study on the LHS joined to multiple studies
				//    in another registry.

				sql_string = @"DROP TABLE IF EXISTS nk.linked_preferred_studies;
                create table nk.linked_preferred_studies as
				select k2.* from
				nk.distinct_links2 k2 inner join
				(select a.source_id, a.sd_id, count(a.preferred_source_id)
					from
						(select distinct k.source_id, k.sd_id, k.preferred_source_id from
								(select source_id, sd_id, count(preferred_source_id) from nk.distinct_links2
									group by source_id, sd_id
									having count(preferred_source_id) > 1) d
						inner join nk.distinct_links2 k
						on d.source_id = k.source_id
						and d.sd_id = k.sd_id) a
					group by a.source_id, a.sd_id
					having count(a.preferred_source_id) = 1) agg
			    on k2.source_id = agg.source_id
				and k2.sd_id = agg.sd_id
				order by k2.source_id, k2.sd_id;";

				conn.Execute(sql_string);

				// In this instance the source_id side is the group and the preferred side 
				// is comprised of the grouped studies.

				sql_string = @"INSERT INTO nk.linked_study_groups 
                             (source_id, source_sd_id, relationship_id, target_sd_id, target_source_id)
                             select source_id, sd_id, 25, preferred_sd_id, preferred_source_id
				             from nk.linked_preferred_studies;";
				conn.Execute(sql_string);

				sql_string = @"INSERT INTO nk.linked_study_groups 
                             (source_id, source_sd_id, relationship_id, target_sd_id, target_source_id)
                             select preferred_source_id, preferred_sd_id, 26, sd_id, source_id
				             from nk.linked_preferred_studies;";
				conn.Execute(sql_string);


				// Now need to delete these grouped records from the links table...
				// Delete all the records that appear in the created table

				sql_string = @"DELETE FROM nk.distinct_links2 k
                               USING nk.linked_preferred_studies s
                               WHERE k.source_id = s.source_id
                               and k.sd_id = s.sd_id;";
				conn.Execute(sql_string);

				sql_string = @"DROP TABLE IF EXISTS nk.linked_preferred_studies;";
				conn.Execute(sql_string);

			}
		}

		public void ManageLinkedNonPreferredSources()
		{
			string sql_string; 
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				// create a table that (working from the inside out)
				// a) gets the preferred id/sd_ids of the RHS of the study links table
				//    that has more than one 'source' study associated with it (dataset d)
				// b) takes those records and identifies the distinct source ids
				//    that are linked to each RHS study (dataset a)
				// c) Further identifies the records that have just a single
				//    source referenced on the LHS (so all the linked records belong
				//    to the same registry (dataset agg)
				// d) joins that dataset back to the linked records table, to 
				//    identify the source records that meet the criteria of 
				//    having one study on the RHS joined to multiple studies
				//    in another registry.
				
				sql_string = @"DROP TABLE IF EXISTS nk.linked_non_preferred_studies;
				create table nk.linked_non_preferred_studies as
				select k2.* from
				nk.distinct_links2 k2 inner join
				(select a.preferred_source_id, a.preferred_sd_id, count(a.source_id)
					from
						(select distinct k.preferred_source_id, k.preferred_sd_id, k.source_id from
								(select preferred_source_id, preferred_sd_id from nk.distinct_links2
										group by preferred_source_id, preferred_sd_id
										having count(source_id) > 1) d
						inner join nk.distinct_links2 k
						on d.preferred_source_id = k.preferred_source_id
						and d.preferred_sd_id = k.preferred_sd_id) a
					group by a.preferred_source_id, a.preferred_sd_id
					having count(a.source_id) = 1) agg
				on k2.preferred_source_id = agg.preferred_source_id
				and k2.preferred_sd_id = agg.preferred_sd_id
				order by k2.preferred_source_id, k2.preferred_sd_id;";

				conn.Execute(sql_string);

				// In this instance the preferred_id side is the group and the source side 
				// is comprised of the grouped studies.

				sql_string = @"INSERT INTO nk.linked_study_groups 
                             (source_id, source_sd_id, relationship_id, target_sd_id, target_source_id)
                             select preferred_source_id, preferred_sd_id, 25, sd_id, source_id
				             from nk.linked_non_preferred_studies;";

				conn.Execute(sql_string);

				sql_string = @"INSERT INTO nk.linked_study_groups 
                             (source_id, source_sd_id, relationship_id, target_sd_id, target_source_id)
                             select source_id, sd_id, 26, preferred_sd_id, preferred_source_id
				             from nk.linked_non_preferred_studies;";

				conn.Execute(sql_string);

				// Now need to delete these grouped records from the links table...
				sql_string = @"DELETE FROM nk.distinct_links2 k
                               USING nk.linked_non_preferred_studies s
                               WHERE k.preferred_source_id = s.preferred_source_id
                               and k.preferred_sd_id = s.preferred_sd_id;";
				conn.Execute(sql_string);

				sql_string = @"DROP TABLE IF EXISTS nk.linked_non_preferred_studies;";
				conn.Execute(sql_string);

			}
		}

		public void ManageIncompleteLinks()
		{

		}

			public void TransferNewLinksToDataTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = @"DROP TABLE IF EXISTS nk.study_study_links;
                          CREATE TABLE nk.study_study_links2 
                          as TABLE nk.distinct_links";

				conn.Execute(sql_string);
			}
		}


		public int ObtainTotalOfNewLinks()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = @"SELECT COUNT(*) FROM nk.distinct_links2";
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

				sql_string = "DROP TABLE IF EXISTS nk.distinct_links2";
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
