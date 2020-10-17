using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;

namespace DataAggregator
{
	public class LoggingDataLayer
	{
		private string connString;
		private string context_connString;
		private Source source;
		private string sql_file_select_string;
		private string host;
		private string user;
		private string password;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// </summary>
		/// 
		public LoggingDataLayer()
		{
			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();

			host = settings["host"];
			user = settings["user"];
			password = settings["password"];

			builder.Host = host;
			builder.Username = user;
			builder.Password = password;
			builder.Database = "mon";
			connString = builder.ConnectionString;

			builder.Database = "context";
			context_connString = builder.ConnectionString;

			sql_file_select_string = "select id, source_id, sd_id, remote_url, last_revised, ";
			sql_file_select_string += " assume_complete, download_status, local_path, last_saf_id, last_downloaded, ";
			sql_file_select_string += " last_harvest_id, last_harvested, last_import_id, last_imported ";

		}

		public Source SourceParameters => source;

		public Source FetchSourceParameters(int source_id)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				source = Conn.Get<Source>(source_id);
				return source;
			}
		}


		public int GetNextAggEventId()
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				string sql_string = "select max(id) from sf.aggregation_events ";
				int? last_id = Conn.ExecuteScalar<int?>(sql_string);
				return (last_id == null) ? 100001 : (int)last_id + 1;
			}
		}


		public int StoreAggregationEvent(AggregationEvent aggregation)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				return (int)conn.Insert<AggregationEvent>(aggregation);
			}

		}


		public IEnumerable<Source> RetrieveDataSources()
		{
			string sql_string = @"select id, preference_rating, database_name, 
                                  has_study_tables,	has_study_topics, has_study_features,
		                          has_study_contributors, has_study_references, has_study_relationships,
		                          has_object_datasets, has_object_dates, has_object_rights,
		                          has_object_relationships, has_object_pubmed_set 
                                from sf.source_parameters
                                where id > 100115
                                order by preference_rating;";

			using (var conn = new NpgsqlConnection(connString))
			{
				return conn.Query<Source>(sql_string);
			}
		}


		public string FetchConnString(string database_name)
		{
		    NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
		    builder.Host = host;
			builder.Username = user;
			builder.Password = password;
			builder.Database = database_name;
			return builder.ConnectionString;
		}


		public int GetRecNum(string table_name, string source_conn_string)
        {
			string test_string = "SELECT to_regclass('ad." + table_name + "')::varchar";
			string table_exists;
			using (var conn = new NpgsqlConnection(source_conn_string))
			{
				table_exists = conn.ExecuteScalar<string>(test_string);
			}
			if (table_exists == null)
			{
				return 0;
			}
			else
			{
				string sql_string = "SELECT count(*) from ad." + table_name;
				int? rec_num;
				using (var conn = new NpgsqlConnection(source_conn_string))
				{
					rec_num = conn.ExecuteScalar<int?>(sql_string);
				}
				return rec_num == null ? 0 : (int)rec_num;
			}
        }

        public void StoreSourceSummary(SourceSummary sm)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Insert<SourceSummary>(sm);
			}
		}


	    // Stores an 'extraction note', e.g. an unusual occurence found and
	    // logged during the extraction, in the associated table.

	    public void StoreExtractionNote(ExtractionNote ext_note)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Insert<ExtractionNote>(ext_note);
			}
		}


	}

}

