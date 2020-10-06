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
				 .MapVarchar("sd_id_1", x => x.sd_id_1)
				 .MapVarchar("sd_id_2", x => x.sd_id_2)
				 .MapInteger("source_2", x => x.source_2);

		public static PostgreSQLCopyHelper<StudyIds> study_ids_helper =
			 new PostgreSQLCopyHelper<StudyIds>("nk", "temp_study_ids")
				 .MapInteger("study_ad_id", x => x.study_ad_id)
				 .MapInteger("study_source_id", x => x.study_source_id)
				 .MapVarchar("study_sd_id", x => x.study_sd_id)
				 .MapVarchar("study_hash_id", x => x.study_hash_id)
				 .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);

		public static PostgreSQLCopyHelper<ObjectIds> object_ids_helper =
			 new PostgreSQLCopyHelper<ObjectIds>("nk", "temp_object_ids")
				 .MapInteger("object_ad_id", x => x.object_ad_id)
				 .MapInteger("object_source_id", x => x.object_source_id)
				 .MapVarchar("object_sd_id", x => x.object_sd_id)
				 .MapVarchar("object_hash_id", x => x.object_hash_id)
				 .MapTimeStampTz("datetime_of_data_fetch", x => x.datetime_of_data_fetch);

		public static PostgreSQLCopyHelper<NewStudyIds> source_study_ids_helper =
			 new PostgreSQLCopyHelper<NewStudyIds>("nk", "temp_study_ids")
				 .MapInteger("study_id", x => x.study_id)
				 .MapInteger("study_ad_id", x => x.study_ad_id)
				 .MapVarchar("study_sd_id", x => x.study_sd_id);

		public static PostgreSQLCopyHelper<NewObjectIds> source_object_ids_helper =
			 new PostgreSQLCopyHelper<NewObjectIds>("nk", "temp_object_ids")
				 .MapInteger("object_id", x => x.object_id)
				 .MapInteger("object_ad_id", x => x.object_ad_id)
				 .MapVarchar("object_sd_id", x => x.object_sd_id)
				 .MapVarchar("object_hash_id", x => x.object_hash_id);

	}
}
