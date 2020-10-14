using Dapper;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class LinksDataHelper
    {
		DataLayer repo;
		string connString;

		public LinksDataHelper(DataLayer _repo)
		{
			repo = _repo;
			connString = repo.ConnString;
		}

		public void SetUpTempLinksBySourceTable()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_links_by_source;
                      CREATE TABLE nk.temp_study_links_by_source(
				        source_1 int
                      , sd_sid_1 varchar
                      , sd_sid_2 varchar
                      , source_2 int) ";
				conn.Execute(sql_string);
			}
		}


		public void SetUpTempPreferencesTable(IEnumerable<Source> sources)
		{
			string sql_string;
			using (var conn = new NpgsqlConnection(connString))
			{
				sql_string = @"DROP TABLE IF EXISTS nk.temp_preferences;
                      CREATE TABLE IF NOT EXISTS nk.temp_preferences(
				        id int
                      , preference_rating int
                      , database_name varchar
                ) ";
				conn.Execute(sql_string);

				List<DataSource> ds = new List<DataSource>();
				foreach (Source s in sources)
                {
					ds.Add(new DataSource(s.id, s.preference_rating, s.database_name));
                }
				conn.Open(); 
				IdCopyHelpers.prefs_helper.SaveAll(conn, ds);
			}
		}


		public void TruncateLinksBySourceTable()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"TRUNCATE TABLE nk.temp_study_links_by_source";
				conn.Execute(sql_string);
			}
		}
		
		
		public IEnumerable<StudyLink> FetchLinks(int source_id, string database_name)
		{
			string conn_string = repo.GetConnString(database_name);

			using (var conn = new NpgsqlConnection(conn_string))
			{
				string sql_string = @"select " + source_id.ToString() + @" as source_1, 
                    sd_sid as sd_sid_1, 
                    identifier_value as sd_sid_2, identifier_org_id as source_2
                    from ad.study_identifiers
                    where identifier_type_id = 11
                    and identifier_org_id > 100115
                    and (identifier_org_id < 100133 or identifier_org_id = 101989)
                    and identifier_org_id <> " + source_id.ToString();
				return conn.Query<StudyLink>(sql_string);
			}
		}


		public ulong StoreLinksInTempTable(PostgreSQLCopyHelper<StudyLink> copyHelper, IEnumerable<StudyLink> entities)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public void TidyISRCTNIds()
		{
			string sql_string = "";
			using (var conn = new NpgsqlConnection(connString))
			{
				sql_string = @"UPDATE nk.temp_study_links_by_source
				SET sd_sid_2 = 'ISRCTN' || sd_sid_2
				WHERE source_2 = 100126
                and length(sd_sid_2) = 8";
				conn.Execute(sql_string);

				sql_string = @"UPDATE nk.temp_study_links_by_source
				SET sd_sid_2 = replace(sd_sid_2, ' ', '')
				WHERE source_2 = 100126";
				conn.Execute(sql_string);

				sql_string = @"UPDATE nk.temp_study_links_by_source
				SET sd_sid_2 = replace(sd_sid_2, '-', '')
				WHERE source_2 = 100126";
				conn.Execute(sql_string);

				sql_string = @"DELETE FROM nk.temp_study_links_by_source
				WHERE source_2 = 100126
                and sd_sid_2 = 'ISRCTN' or sd_sid_2 = 'ISRCTN00000000'";
				conn.Execute(sql_string);
			}
		}


		public void TidyNCTIds()
		{
			string sql_string = "";
			using (var conn = new NpgsqlConnection(connString))
			{
				sql_string = @"UPDATE nk.temp_study_links_by_source
				SET sd_sid_2 = replace(sd_sid_2, ' ', '')
				WHERE source_2 = 100120";
				conn.Execute(sql_string);

				sql_string = @"UPDATE nk.temp_study_links_by_source
				SET sd_sid_2 = replace(sd_sid_2, '-', '')
				WHERE source_2 = 100120";
				conn.Execute(sql_string);

				sql_string = @"DELETE FROM nk.temp_study_links_by_source
				WHERE source_2 = 100120
                and (sd_sid_2 = 'NCT00000000' or sd_sid_2 = 'NCT99999999' or 
                sd_sid_2 = 'NCT12345678' or sd_sid_2 = 'NCT87654321' or 
                lower(sd_sid_2) = 'na' or lower(sd_sid_2) = 'n/a');";
				conn.Execute(sql_string);
			}
		}


		public void SetUpTempLinkCollectorTable()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_links_collector;
                        CREATE TABLE nk.temp_study_links_collector(
				        source_id int
                      , sd_sid varchar
                      , preferred_sd_sid varchar
                      , preferred_source_id int) ";
				conn.Execute(sql_string);
			}
		}


		public void TransferLinksToCollectorTable()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// needs to be done twice to keep the ordering of sources correct
				// A lower rating means 'more preferred' - i.e. should be used in preference
				// Therefore lower rated source data should be in the 'preferred' fields
				// and higher rated data should be on the left hand side

				// Original data matches what is required

				string sql_string = @"INSERT INTO nk.temp_study_links_collector(
				          source_id, sd_sid, preferred_sd_sid, preferred_source_id) 
                          SELECT t.source_1, t.sd_sid_1, t.sd_sid_2, t.source_2
						  FROM nk.temp_study_links_by_source t
                          inner join nk.temp_preferences r1
                          on t.source_1 = r1.id
                          inner join nk.temp_preferences r2
                          on t.source_2 = r2.id
                          WHERE r1.preference_rating > r2.preference_rating";

				conn.Execute(sql_string);

				// Original data is the opposite of what is required - therefore switch

				sql_string = @"INSERT INTO nk.temp_study_links_collector(
				          source_id, sd_sid, preferred_sd_sid, preferred_source_id) 
                          SELECT t.source_2, t.sd_sid_2, t.sd_sid_1, t.source_1
						  FROM nk.temp_study_links_by_source t
                          inner join nk.temp_preferences r1
                          on t.source_1 = r1.id
                          inner join nk.temp_preferences r2
                          on t.source_2 = r2.id
                          WHERE r1.preference_rating < r2.preference_rating";

				conn.Execute(sql_string);
			}
		}


		public void CreateDistinctSourceLinksTable()
		{
			// The nk.temp_study_links_collector table will have 
			// many duplicates... create a distinct version of the data

			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"DROP TABLE IF EXISTS nk.temp_distinct_links;
                           CREATE TABLE nk.temp_distinct_links 
                           as SELECT distinct source_id, sd_sid, preferred_sd_sid, preferred_source_id
						   FROM nk.temp_study_links_collector";

				conn.Execute(sql_string);
			}
		}


		public void CascadeLinksInDistinctLinksTable()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				// telescope the preferred links to the most preferred
				// i.e. A -> B, B -> C becomes A -> C, B -> C
				// do as long as there remains links to be telescoped
				// (a few have to be done twice)

				string sql_string;
				int match_number = 500;  // arbitrary start number
				while (match_number > 0)
				{
					// get match number as number of link records where the rhs sd_sid
					// appears elsewhere on the left...

					  sql_string = @"SELECT count(*) 
						  FROM nk.temp_distinct_links t1
                          inner join nk.temp_distinct_links t2
                          on t1.preferred_source_id = t2.source_id
                          and t1.preferred_sd_sid = t2.sd_sid";

					match_number = conn.ExecuteScalar<int>(sql_string);

					if (match_number > 0)
					{
						// do the update

					    sql_string = @"UPDATE nk.temp_distinct_links t1
                          SET preferred_source_id = t2.preferred_source_id,
                          preferred_sd_sid = t2.preferred_sd_sid
						  FROM nk.temp_distinct_links t2
                          WHERE t1.preferred_source_id = t2.source_id
                          AND t1.preferred_sd_sid = t2.sd_sid";

						conn.Execute(sql_string);
					}
				}

				// but in some cases the telescoped link may have already been 
				// present - i.e. a study has two other reg identifiers
				// one of which will be the most preferred
				// Process above will result in duplicates in these cases
				// and these duplicates therefore need to be removed.

				sql_string = @"DROP TABLE IF EXISTS nk.temp_distinct_links2;
                           CREATE TABLE nk.temp_distinct_links2 
                           as SELECT distinct * FROM nk.temp_distinct_links";

				conn.Execute(sql_string);

				sql_string = @"DROP TABLE IF EXISTS nk.temp_distinct_links;
				ALTER TABLE nk.temp_distinct_links2 RENAME TO temp_distinct_links;";

                conn.Execute(sql_string);
			}
		}


		// one set of relationships are not 'same study in a different registry'
		// but multiple studies in a different registry

		// such studies have a study relationship rather than being straight equivalents...
		// there can be multiple studies in the 'preferred' registry
		// or in the non-preferred registry - each group being equivalent to
		// a registry entry that represents a single study, or sometimes a 
		// single project / programme, or grant

		// The study relationships are 
		// (source) 26: Is a member of a group of 2 or more studies registered elsewhere as a single study, (the referenced study).
		// (source) 25: Includes, as a member of a related group registered individually elsewhere, the referenced study.

		public void ManageLinkedPreferredSources()
		{
			string sql_string;

			using (var conn = new NpgsqlConnection(connString))
			{
				// create a table that (working from the inside out)
				// a) gets the source id/sd_sids of thte LHS of the study links table
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
				nk.temp_distinct_links k2 inner join
				(select a.source_id, a.sd_sid, count(a.preferred_source_id)
					from
						(select distinct k.source_id, k.sd_sid, k.preferred_source_id from
								(select source_id, sd_sid, count(preferred_source_id) from nk.temp_distinct_links
									group by source_id, sd_sid
									having count(preferred_source_id) > 1) d
						inner join nk.temp_distinct_links k
						on d.source_id = k.source_id
						and d.sd_sid = k.sd_sid) a
					group by a.source_id, a.sd_sid
					having count(a.preferred_source_id) = 1) agg
			    on k2.source_id = agg.source_id
				and k2.sd_sid = agg.sd_sid
				order by k2.source_id, k2.sd_sid;";

				conn.Execute(sql_string);

				// In this instance the source_id side is the group and the preferred side 
				// is comprised of the grouped studies.

				sql_string = @"INSERT INTO nk.linked_study_groups 
                             (source_id, source_sd_sid, relationship_id, target_sd_sid, target_source_id)
                             select source_id, sd_sid, 25, preferred_sd_sid, preferred_source_id
				             from nk.linked_preferred_studies;";
				conn.Execute(sql_string);

				sql_string = @"INSERT INTO nk.linked_study_groups 
                             (source_id, source_sd_sid, relationship_id, target_sd_sid, target_source_id)
                             select preferred_source_id, preferred_sd_sid, 26, sd_sid, source_id
				             from nk.linked_preferred_studies;";
				conn.Execute(sql_string);

				// Now need to delete these grouped records from the links table...
				// Delete all the records that appear in the created table

				sql_string = @"DELETE FROM nk.temp_distinct_links k
                               USING nk.linked_preferred_studies s
                               WHERE k.source_id = s.source_id
                               and k.sd_sid = s.sd_sid;";
				conn.Execute(sql_string);

				sql_string = @"DROP TABLE IF EXISTS nk.linked_preferred_studies;";
				conn.Execute(sql_string);

			}
		}

		public void ManageLinkedNonPreferredSources()
		{
			string sql_string; 
			using (var conn = new NpgsqlConnection(connString))
			{
				// create a table that (working from the inside out)
				// a) gets the preferred id/sd_sids of the RHS of the study links table
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
				nk.temp_distinct_links k2 inner join
				(select a.preferred_source_id, a.preferred_sd_sid, count(a.source_id)
					from
						(select distinct k.preferred_source_id, k.preferred_sd_sid, k.source_id from
								(select preferred_source_id, preferred_sd_sid from nk.temp_distinct_links
										group by preferred_source_id, preferred_sd_sid
										having count(source_id) > 1) d
						inner join nk.temp_distinct_links k
						on d.preferred_source_id = k.preferred_source_id
						and d.preferred_sd_sid = k.preferred_sd_sid) a
					group by a.preferred_source_id, a.preferred_sd_sid
					having count(a.source_id) = 1) agg
				on k2.preferred_source_id = agg.preferred_source_id
				and k2.preferred_sd_sid = agg.preferred_sd_sid
				order by k2.preferred_source_id, k2.preferred_sd_sid;";

				conn.Execute(sql_string);

				// In this instance the preferred_id side is the group and the source side 
				// is comprised of the grouped studies.

				sql_string = @"INSERT INTO nk.linked_study_groups 
                             (source_id, source_sd_sid, relationship_id, target_sd_sid, target_source_id)
                             select preferred_source_id, preferred_sd_sid, 25, sd_sid, source_id
				             from nk.linked_non_preferred_studies;";

				conn.Execute(sql_string);

				sql_string = @"INSERT INTO nk.linked_study_groups 
                             (source_id, source_sd_sid, relationship_id, target_sd_sid, target_source_id)
                             select source_id, sd_sid, 26, preferred_sd_sid, preferred_source_id
				             from nk.linked_non_preferred_studies;";

				conn.Execute(sql_string);

				// Now need to delete these grouped records from the links table...

				sql_string = @"DELETE FROM nk.temp_distinct_links k
                               USING nk.linked_non_preferred_studies s
                               WHERE k.preferred_source_id = s.preferred_source_id
                               and k.preferred_sd_sid = s.preferred_sd_sid;";
				conn.Execute(sql_string);

				sql_string = @"DROP TABLE IF EXISTS nk.linked_non_preferred_studies;";
				conn.Execute(sql_string);

			}
		}

		public void ManageIncompleteLinks()
		{

			// There are a set if links that are missing, in the sense that
			// Study A is listed as being the same as Study B and Study C, but no
			// link exists beteween either Study B to C, or Study C to B.
			// The 'link path' is therefore broken and the B to C link needs to be added.
			// These studies have two, occasionally more, 'preferred studies', which
			// does not make sense in the system.

			// First create a table with these 'missing link' records

			// Working from the inside out, this query
			// a) gets the source id/sd_sids of the LHS of the study links table
			//    that has more than one 'preferred' study associated with it (dataset d)
			// b) takes those records and identifies the distinct preferred source ids
			//    that are linked to each RHS study (dataset a)
			// c) Further identifies the records that have more than one 
			//    source referenced on the RHS (so all the linked records have the 
			//    'impossible' property of having more than one preferred 
			//    source / sd_sid study record (dataset agg)
			// d) joins that dataset back to the linked records table, to 
			//    identify the source records that meet the criteria of 
			//    having a 'missing link'

			string sql_string;
			using (var conn = new NpgsqlConnection(connString))
			{
				sql_string = @"DROP TABLE IF EXISTS nk.temp_missing_links;
                    CREATE TABLE nk.temp_missing_links as
                    select k2.source_id, r1.preference_rating as source_rating, 
                    k2.sd_sid, k2.preferred_source_id, r2.preference_rating, k2.preferred_sd_sid from
                    nk.temp_distinct_links k2 inner join
					(select a.source_id, a.sd_sid, count(a.preferred_source_id)
					from
						(select distinct k.source_id, k.sd_sid, k.preferred_source_id from
								(select source_id, sd_sid, count(preferred_source_id) from nk.temp_distinct_links
										group by source_id, sd_sid
										having count(preferred_source_id) > 1) d
						inner join nk.temp_distinct_links k
						on d.source_id = k.source_id
						and d.sd_sid = k.sd_sid) a
					group by a.source_id, a.sd_sid
					having count(a.preferred_source_id) > 1
					order by a.source_id, a.sd_sid) agg
				on k2.source_id = agg.source_id
				and k2.sd_sid = agg.sd_sid
				inner join nk.temp_preferences r1
				on k2.source_id = r1.id
				inner join nk.temp_preferences r2
				on k2.preferred_source_id = r2.id
				order by k2.source_id, k2.sd_sid, preferred_source_id;";

				conn.Execute(sql_string);

				// Create a further temp table that will hold the links between studies B and C,
				// which are currently both 'preferred' studies (both on the RHS of the table)
				// for any particular source id / sd_sid study.

				// This table is initially populated with the source id / sd_sid, to identify the record
				// and then the preferred source / sd_sid pair that does NOT have the minimum
				// source rating, i.e. is the study that will need to be 'non-preferred' in the new link

				sql_string = @"DROP TABLE IF EXISTS nk.temp_new_links;
				CREATE TABLE nk.temp_new_links as
				select m.source_id, m.sd_sid, m.preferred_source_id as new_source_id, 
                m.preferred_sd_sid as new_sd_sid, 0 as new_preferred_source, '' as new_preferred_sd_sid from
				nk.temp_missing_links m
				inner join
					(select source_id, sd_sid, min(preference_rating) as min_rating
	                 from nk.temp_missing_links
	                 group by source_id, sd_sid) mins
                on m.source_id = mins.source_id
                and m.sd_sid = mins.sd_sid
                and m.preference_rating <> mins.min_rating
                order by source_id, sd_sid;";

				conn.Execute(sql_string);

				// Update the temp_new_links table with the source / sd_sid that represents the
				// study with the mninimally rated source id, i.e. which is the 'correct' preferred option

				sql_string = @"UPDATE nk.temp_new_links k
                      SET new_preferred_source = min_set.preferred_source_id
                      , new_preferred_sd_sid = min_set.preferred_sd_sid
                      FROM
	                  (select m.* from
	                  nk.temp_missing_links m
	                  INNER JOIN 
			             (select source_id, sd_sid, min(preference_rating) as min_rating
		                 from nk.temp_missing_links
		                 group by source_id, sd_sid) mins
	                  on m.source_id = mins.source_id
	                  and m.sd_sid = mins.sd_sid
	                  and m.preference_rating = mins.min_rating) min_set
                      WHERE k.source_id = min_set.source_id
                      AND k.sd_sid = min_set.sd_sid;";

				conn.Execute(sql_string);

				// Insert the new links into the distinct_links table.
				// These links will need re-processing through the CascadeLinksTable() function.

				sql_string = @"INSERT INTO nk.temp_distinct_links
                     (source_id, sd_sid, preferred_sd_sid, preferred_source_id)
                     SELECT new_source_id, new_sd_sid, new_preferred_sd_sid, new_preferred_source from
                     nk.temp_new_links;";

				conn.Execute(sql_string);

				// drop the temp tables 
				sql_string = @"DROP TABLE IF EXISTS nk.temp_missing_links;
				DROP TABLE IF EXISTS nk.temp_new_links;";

				conn.Execute(sql_string);
			}
		}


		public void TransferNewLinksToDataTable()
		{
			// A disitinct selection is required because the most recent
			// link cascade may have generated duplicates

			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"Insert into nk.study_study_links
                      (source_id, sd_sid, preferred_sd_sid, preferred_source_id)
                      select distinct source_id, sd_sid, preferred_sd_sid, preferred_source_id
                      from nk.temp_distinct_links";

				conn.Execute(sql_string);
			}
		}


		public int ObtainTotalOfNewLinks()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"SELECT COUNT(*) FROM nk.temp_distinct_links";
				return conn.ExecuteScalar<int>(sql_string);
			}
		}


		public void DropTempTables()
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = @"DROP TABLE IF EXISTS nk.temp_study_links_by_source;
                DROP TABLE IF EXISTS nk.temp_preferences;
                DROP TABLE IF EXISTS nk.temp_study_links_collector;
				DROP TABLE IF EXISTS nk.temp_distinct_links;";
				conn.Execute(sql_string);
			}
		}

	}
}
