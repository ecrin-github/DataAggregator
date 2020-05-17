using System;
using static System.Console;

namespace DataAggregator
{
    class Program
    {
		static void Main(string[] args)
		{
			string source;
			bool create_tables = false;

			if (args.Length == 0 || args.Length > 2)
			{
				WriteLine("sorry - one or two parameters are necessary");
				WriteLine("The first a string to indicate the source");
				WriteLine("The second either 0 or 1 to indicate whether tables need to be recreated");
			}
			else
			{
				source = args[0];
				if (source != "b" && source != "y")
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
							create_tables = true; // recreate tables
						}
					}

                    // proceed with one or two valid aprameters
			        DataLayer repo = new DataLayer();
					int source_id;
					switch (source)
					{
						case "b":
							{
								source_id = 100900;
								Controller biolincc_controller = new Controller(repo, source_id);
								//biolincc_controller.UpdateStudyLinkList();
								biolincc_controller.EstablishStudyIds(source_id);
								biolincc_controller.LoadStudyData(source_id);
								biolincc_controller.EstablishObjectIds(source_id);
								biolincc_controller.LoadObjectData(source_id);
								biolincc_controller.DropTempTables();
								break;
							}
						case "y":
							{
								source_id = 100901;
								Controller yoda_controller = new Controller(repo, source_id);
								//yoda_controller.UpdateStudyLinkList();
								yoda_controller.EstablishStudyIds(source_id);
								break;
						}
					}



				}
			}


			
			}
		}
}
