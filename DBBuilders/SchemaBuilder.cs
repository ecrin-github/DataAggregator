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
	public class SchemaBuilder
	{
		private string connString;
		private StudyTableBuilder study_tablebuilder;
		private ObjectTableBuilder object_tablebuilder;
		private LinkTableBuilder link_tablebuilder;

		public SchemaBuilder(string _connString)
		{
			connString = _connString;
			study_tablebuilder = new StudyTableBuilder(connString);
			object_tablebuilder = new ObjectTableBuilder(connString);
			link_tablebuilder = new LinkTableBuilder(connString);
		}

		public void DeleteStudyTables()
		{
			// dropping routines include 'if exists'
			// therefore can attempt to drop all of them
			study_tablebuilder.drop_table("studies");
			study_tablebuilder.drop_table("study_identifiers");
			study_tablebuilder.drop_table("study_titles");
			study_tablebuilder.drop_table("study_contributors");
			study_tablebuilder.drop_table("study_topics");
			study_tablebuilder.drop_table("study_features");
			study_tablebuilder.drop_table("study_relationships");
			study_tablebuilder.drop_table("study_references");
			study_tablebuilder.drop_table("study_links");
			study_tablebuilder.drop_table("study_ipd_available");
			study_tablebuilder.drop_table("study_hashes");
		}

		public void DeleteObjectTables()
		{
			// dropping routines include 'if exists'
			// therefore can attempt to drop all of them
			object_tablebuilder.drop_table("data_objects");
			object_tablebuilder.drop_table("object_datasets");
			object_tablebuilder.drop_table("object_dates");
			object_tablebuilder.drop_table("object_instances");
			object_tablebuilder.drop_table("object_titles");
			object_tablebuilder.drop_table("object_contributors");
			object_tablebuilder.drop_table("object_topics");
			object_tablebuilder.drop_table("object_comments");
			object_tablebuilder.drop_table("object_descriptions");
			object_tablebuilder.drop_table("object_identifiers");
			object_tablebuilder.drop_table("object_db_links");
			object_tablebuilder.drop_table("object_publication_types");
			object_tablebuilder.drop_table("object_relationships");
			object_tablebuilder.drop_table("object_rights");
			object_tablebuilder.drop_table("citation_objects");
			object_tablebuilder.drop_table("object_hashes");
		}

		public void DeleteLinkTables()
		{


		}

		public void BuildNewStudyTables()
		{
			// these common to all databases

			study_tablebuilder.create_table_studies();
			study_tablebuilder.create_table_study_identifiers();
			study_tablebuilder.create_table_study_titles();
			study_tablebuilder.create_table_study_hashes();

			study_tablebuilder.create_table_study_topics();
			study_tablebuilder.create_table_study_features();
			study_tablebuilder.create_table_study_contributors();
			study_tablebuilder.create_table_study_references();
			study_tablebuilder.create_table_study_relationships();
			study_tablebuilder.create_table_study_links();
			study_tablebuilder.create_table_ipd_available();

		}


		public void BuildNewObjectTables()
		{
			// these common to all databases

			object_tablebuilder.create_table_data_objects();
			object_tablebuilder.create_table_object_instances();
			object_tablebuilder.create_table_object_titles();
			object_tablebuilder.create_table_object_hashes();

			// these are database dependent		

			object_tablebuilder.create_table_object_datasets();
			object_tablebuilder.create_table_object_dates();
			object_tablebuilder.create_table_object_relationships();
			object_tablebuilder.create_table_object_rights();

			object_tablebuilder.create_table_citation_objects();
			object_tablebuilder.create_table_object_contributors();
			object_tablebuilder.create_table_object_topics();
			object_tablebuilder.create_table_object_comments();
			object_tablebuilder.create_table_object_descriptions();
			object_tablebuilder.create_table_object_identifiers();
			object_tablebuilder.create_table_object_db_links();
			object_tablebuilder.create_table_object_publication_types();
		}


		public void BuildNewLinkTables()
		{


		}

	}
}

