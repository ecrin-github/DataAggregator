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
							create_study_links = true; // recreate tables
						}
					}

					// proceed with one or two valid aprameters
					DataLayer repo = new DataLayer();
					int source_id = 0;

					if (source == "b") source_id = 100900;
					if (source == "y") source_id = 100901;

					Controller controller = new Controller(repo, source_id);
					if (create_study_links)
					{
						controller.UpdateStudyLinkList();
					}
					StudyDataTransfer study_trans = new StudyDataTransfer();
					controller.EstablishStudyIds(study_trans, source_id);
					controller.LoadStudyData(study_trans, source_id);
					ObjectDataTransfer object_trans = new ObjectDataTransfer();
					controller.EstablishObjectIds(object_trans, source_id);
					controller.LoadObjectData(object_trans, source_id);
					controller.DropTempTables(study_trans, object_trans);
				}
			}
		}
	}
}
