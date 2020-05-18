using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
	public class ObjectDataTransfer
    {

		private string mdr_nk_connString;
		private string biolincc_ad_connString;
		private string yoda_ad_connString;
		private string ctg_ad_connString;
		private string euctr_ad_connString;
		private string isrctn_ad_connString;
		private string who_ad_connString;
		private string mon_sf_connString;

		public ObjectDataTransfer()
		{
			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];

			builder.Database = "mdr";
			builder.SearchPath = "nk";
			mdr_nk_connString = builder.ConnectionString;

			builder.Database = "biolincc";
			builder.SearchPath = "ad";
			biolincc_ad_connString = builder.ConnectionString;

			builder.Database = "ctg";
			builder.SearchPath = "ad";
			ctg_ad_connString = builder.ConnectionString;

			builder.Database = "yoda";
			builder.SearchPath = "ad";
			yoda_ad_connString = builder.ConnectionString;

			builder.Database = "euctr";
			builder.SearchPath = "ad";
			euctr_ad_connString = builder.ConnectionString;

			builder.Database = "isrctn";
			builder.SearchPath = "ad";
			isrctn_ad_connString = builder.ConnectionString;

			builder.Database = "who";
			builder.SearchPath = "ad";
			who_ad_connString = builder.ConnectionString;

			builder.Database = "mon";
			builder.SearchPath = "sf";
			mon_sf_connString = builder.ConnectionString;
		}


		public string GetConnString(int org_id)
		{
			string conn_string = "";
			switch (org_id)
			{
				case 100120: { conn_string = ctg_ad_connString; break; }
				case 100123: { conn_string = euctr_ad_connString; break; }
				case 100126: { conn_string = isrctn_ad_connString; break; }
				case 100900: { conn_string = biolincc_ad_connString; break; }
				case 100901: { conn_string = yoda_ad_connString; break; }
				case 100115: { conn_string = who_ad_connString; break; }
			}
			return conn_string;
		}



		public void SetUpTempObjectIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = @"CREATE TABLE IF NOT EXISTS nk.temp_object_ids(
				        object_id int
                      , object_ad_id int
                      , object_source_id int
                      , object_sd_id varchar
                      , object_hash_id varchar
                      , datetime_of_data_fetch timestamptz
                      , parent_study_id int
                      , is_new boolean
                      , is_study_new boolean
                      ); ";
				conn.Execute(sql_string);
			}
		}



		public IEnumerable<ObjectIds> FetchObjectIds(int org_id)
		{
			string conn_string = GetConnString(org_id);
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
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}


		public void LoadObjectData(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectDatasets(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectInstances(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectTitles(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectDates(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectContributors(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectTopics(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}

		public void LoadObjectRelationships(int org_id)
		{
			string conn_string = GetConnString(org_id);
		}



		public void DropTempObjectIdsTable()
		{
			using (var conn = new NpgsqlConnection(mdr_nk_connString))
			{
				string sql_string = "DROP TABLE IF EXISTS nk.temp_object_ids";
				conn.Execute(sql_string);
			}
		}

	}
}
