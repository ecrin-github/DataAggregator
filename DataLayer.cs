using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using PostgreSQLCopyHelper;

namespace DataAggregator
{
	public class DataLayer
	{
		private string mdr_connString;
		private string biolincc_ad_connString;
		private string yoda_ad_connString;
		private string ctg_ad_connString;
		private string euctr_ad_connString;
		private string isrctn_ad_connString;
		private string who_ad_connString;
		private string user_name;
		private string password;


		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// The json file also includes the root folder path, which is
		/// stored in the class's folder_base property.
		/// </summary>
		public DataLayer()
		{
			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];

			user_name = settings["user"];
			password = settings["password"];

			builder.Database = "mdr";
			builder.SearchPath = "nk";
			mdr_connString = builder.ConnectionString;

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


			// example appsettings.json file...
			// the only values required are for...
			// {
			//	  "host": "host_name...",
			//	  "user": "user_name...",
			//    "password": "user_password...",
			//	  "folder_base": "C:\\MDR JSON\\Object JSON... "
			// }
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

		public string GetMDRConnString()
		{
			return mdr_connString;
		}

		// these occasionally have to be used in SQL statements
		public string GetUserName()
		{
			return user_name;
		}

		public string GetPassword()
		{
			return password;
		}

	}


}
