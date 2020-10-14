using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public static class IdCopyHelpers
    {

		public static PostgreSQLCopyHelper<StudyLink> links_helper =
			 new PostgreSQLCopyHelper<StudyLink>("nk", "temp_study_links_by_source")
				 .MapInteger("source_1", x => x.source_1)
				 .MapVarchar("sd_sid_1", x => x.sd_sid_1)
				 .MapVarchar("sd_sid_2", x => x.sd_sid_2)
				 .MapInteger("source_2", x => x.source_2);


		public static PostgreSQLCopyHelper<DataSource> prefs_helper =
			 new PostgreSQLCopyHelper<DataSource>("nk", "temp_preferences")
				 .MapInteger("id", x => x.id)
				 .MapInteger("preference_rating", x => x.preference_rating)
				 .MapVarchar("database_name", x => x.database_name);


		public static PostgreSQLCopyHelper<StudyIds> study_ids_helper =
			 new PostgreSQLCopyHelper<StudyIds>("nk", "temp_study_ids")
				 .MapInteger("source_id", x => x.source_id)
				 .MapVarchar("sd_sid", x => x.sd_sid)
				 .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);


		public static PostgreSQLCopyHelper<ObjectIds> object_ids_helper =
			 new PostgreSQLCopyHelper<ObjectIds>("nk", "temp_object_ids")
				 .MapInteger("source_id", x => x.source_id)
				 .MapVarchar("sd_oid", x => x.sd_oid)
				 .MapVarchar("parent_sd_sid", x => x.parent_sd_sid)
				 .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);


		/*
		public static PostgreSQLCopyHelper<NewStudyIds> source_study_ids_helper =
			 new PostgreSQLCopyHelper<NewStudyIds>("nk", "temp_study_ids")
				 .MapInteger("study_id", x => x.study_id)
				 .MapInteger("source_id", x => x.source_id)
				 .MapVarchar("sd_sid", x => x.sd_sid)
			     .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);

		public static PostgreSQLCopyHelper<NewObjectIds> source_object_ids_helper =
			 new PostgreSQLCopyHelper<NewObjectIds>("nk", "temp_object_ids")
				 .MapInteger("object_id", x => x.object_id)
				 .MapInteger("source_id", x => x.source_id)
				 .MapVarchar("sd_oid", x => x.sd_oid)
				 .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);
		*/
	}
}
