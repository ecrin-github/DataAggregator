using System;
using static System.Console;

namespace DataAggregator
{
	class Program
	{
		static void Main(string[] args)
		{
			string source;
			bool create_study_links = false;

			if (args.Length == 0 || args.Length > 2)
			{
				WriteLine("sorry - one or two parameters are necessary");
				WriteLine("The first a string to indicate the source");
				WriteLine("The second (optional) either 0 or 1 to indicate whether study link data need to be recreated");
			}
			else
			{
				source = args[0];
				if (source != "b" && source != "y" && source != "c" && source != "e" 
					   && source != "i" && source != "w")
				{
					WriteLine("sorry - I don't recognise that source argument");
					return;
				}
				else
				{
					if (args.Length == 2)
					{
						// should be '0' or '1'
						// to indicate create new ad tables use 1 
						// default is 0, leave files as they are!
						string table_create = args[1];
						if (table_create == "1")
						{
							create_study_links = true; // recreate tables
						}
					}

					// proceed with one or two valid aprameters
					DataLayer repo = new DataLayer();
					int source_id = 0; string database_name = "";

					switch (source)
					{
						case "b":
							{
								database_name = "biolincc";
								source_id = 100900; 
								break;
							}
						case "y":
							{
								database_name = "yoda";
								source_id = 100901; 
								break;
							}
						case "c":
							{
								database_name = "ctg";
								source_id = 100120;
								break;
							}
						case "e":
							{
								database_name = "euctr";
								source_id = 100123;
								break;
							}
						case "i":
							{
								database_name = "isrctn";
								source_id = 100126;
								break;
							}
						case "w":
							{
								database_name = "who";
								source_id = 100115;
								break;
							}
					}

					Controller controller = new Controller(repo, source_id);
					if (create_study_links)
					{
						controller.UpdateStudyLinkList();
					}

					string schema_name = controller.SetUpTempSchema(database_name);
					//controller.EstablishStudyIds(source_id);
					controller.LoadStudyData(schema_name);
					controller.EstablishObjectIds(source_id);
					controller.LoadObjectData(schema_name);
					controller.DropTempTables();
					controller.DropTempSchema(schema_name);
				}
			}
		}
	}
}
