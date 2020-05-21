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
		string mdr_connString;

		public ObjectDataTransferrer(DataLayer _repo)
		{
			repo = _repo;
			mdr_connString = repo.GetMDRConnString();
		}

		public void SetUpTempObjectIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = @"CREATE TABLE IF NOT EXISTS nk.temp_object_ids(
				        object_id int
                      , object_ad_id int
                      , object_source_id int
                      , object_sd_id varchar
                      , object_hash_id varchar
                      , datetime_of_data_fetch timestamptz
                      , parent_study_id int
                      , is_preferred boolean
                      , is_study_new boolean
                      ); ";
				conn.Execute(sql_string);
			}
		}



		public IEnumerable<ObjectIds> FetchObjectIds(int org_id)
		{
			string conn_string = repo.GetConnString(org_id);
			using (var conn = new NpgsqlConnection(conn_string))
			{
				string sql_string = @"select ad_id as object_ad_id, " + org_id.ToString() + @" as object_source_id, 
                          sd_id as object_sd_id, object_hash_id, datetime_of_data_fetch
                          from ad.data_objects
                          where record_status_id = 1";

				return conn.Query<ObjectIds>(sql_string);
			}
		}


		public ulong StoreObjectIds(PostgreSQLCopyHelper<ObjectIds> copyHelper, IEnumerable<ObjectIds> entities)
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public void LoadObjectData(string schema_name)
		{

		}

		public void LoadObjectDatasets(string schema_name)
		{

		}

		public void LoadObjectInstances(string schema_name)
		{

		}

		public void LoadObjectTitles(string schema_name)
		{

		}

		public void LoadObjectDates(string schema_name)
		{

		}

		public void LoadObjectContributors(string schema_name)
		{

		}

		public void LoadObjectTopics(string schema_name)
		{

		}

		public void LoadObjectRelationships(string schema_name)
		{

		}



		public void DropTempObjectIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_object_ids";
				conn.Execute(sql_string);
			}
		}

	}
}
