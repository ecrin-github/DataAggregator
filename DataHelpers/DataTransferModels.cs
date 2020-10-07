using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
	public class StudyLink
	{
		public int source_1 { get; set; }
		public string sd_sid_1 { get; set; }
		public string sd_sid_2 { get; set; }
		public int source_2 { get; set; }
	}




	public class StudyIds
	{
		public int study_ad_id { get; set; }
		public int study_source_id { get; set; }
		public string study_sd_id { get; set; }
		public string study_hash_id { get; set; }
		public DateTime? datetime_of_data_fetch { get; set; }
	}


	public class ObjectIds
	{
		public int object_ad_id { get; set; }
		public int object_source_id { get; set; }
		public string object_sd_id { get; set; }
		public string object_hash_id { get; set; }
		public DateTime? datetime_of_data_fetch { get; set; }
	}


	public class NewStudyIds
	{
		public int study_id { get; set; }
		public int study_ad_id { get; set; }
		public string study_sd_id { get; set; }
	}


	public class NewObjectIds
	{
		public int object_id { get; set; }
		public int object_ad_id { get; set; }
		public string object_sd_id { get; set; }
		public string object_hash_id { get; set; }
	}
}
