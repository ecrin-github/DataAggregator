using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;

namespace DataAggregator
{
	public class DataLayer
	{
		private string connString;
		private string mon_connString;
		private string username;
		private string password;
		private string host;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// </summary>
		/// 
		public DataLayer(string database_name)
		{
				IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];
			builder.Database = database_name;

			connString = builder.ConnectionString;

			builder.Database = "mon";
			mon_connString = builder.ConnectionString;

			username = builder.Username;
			password = builder.Password;
			host = builder.Host;
		}

		public string ConnString => connString;
		public string monConnString => mon_connString;

		public string Username => username;
		public string Password => password;


		public string GetConnString(int source_id)
		{
			string sql_string = @"select database_name 
                                  from sf.source_parameters 
                                  where id = " + source_id.ToString();
			string db_name = "";
			using (var conn = new NpgsqlConnection(mon_connString))
			{
				db_name = conn.Query<string>(sql_string).FirstOrDefault();
			}

			return GetConnString(db_name);
		}


		public string GetConnString(string db_name)
		{
			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = host;
			builder.Username = username;
			builder.Password = password;
			builder.Database = db_name;
			return builder.ConnectionString;
		}

	}
}

