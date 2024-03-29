﻿using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public class Credentials : ICredentials
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        public Credentials(IConfiguration settings)
        {
            Host = settings["host"];
            Username = settings["user"];
            Password = settings["password"];
        }

        public string GetConnectionString(string database_name, bool testing)
        {
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
            builder.Host = Host;
            builder.Username = Username;
            builder.Password = Password;
            builder.Database = (testing) ? "test" : database_name;
            return builder.ConnectionString;
        }
    }
}
