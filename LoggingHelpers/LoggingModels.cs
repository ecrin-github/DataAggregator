using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;

namespace DataAggregator
{
	[Table("sf.source_parameters")]
	public class Source
	{
		public int id { get; set; }
		public int? preference_rating { get; set; }
		public string database_name { get; set; }
		public int default_harvest_type_id { get; set; }
		public bool requires_file_name { get; set; }
		public bool uses_who_harvest { get; set; }
		public string local_folder { get; set; }
		public bool? local_files_grouped { get; set; }
		public int? grouping_range_by_id { get; set; }
		public string local_file_prefix { get; set; }
		public bool has_study_tables { get; set; }
		public bool has_study_topics { get; set; }
		public bool has_study_features { get; set; }
		public bool has_study_contributors { get; set; }
		public bool has_study_references { get; set; }
		public bool has_study_relationships { get; set; }
		public bool has_study_links { get; set; }
		public bool has_study_ipd_available { get; set; }
		public bool has_object_datasets { get; set; }
		public bool has_object_dates { get; set; }
		public bool has_object_rights { get; set; }
		public bool has_object_relationships { get; set; }
		public bool has_object_pubmed_set { get; set; }
	}

	[Table("sf.extraction_notes")]
	public class ExtractionNote
	{
		public int id { get; set; }
		public int source_id { get; set; }
		public string sd_id { get; set; }
		public string event_type { get; set; }
		public int event_type_id { get; set; }
		public int? note_type_id { get; set; }
		public string note { get; set; }

		public ExtractionNote(int _source_id, string _sd_id, string _event_type,
							  int _event_type_id, int? _note_type_id, string _note)
		{
			source_id = _source_id;
			sd_id = _sd_id;
			event_type = _event_type;
			event_type_id = _event_type_id;
			note_type_id = _note_type_id;
			note = _note;
		}
	}


	[Table("sf.aggregation_events")]
	public class AggregationEvent
	{
		[ExplicitKey]
		public int id { get; set; }
		public DateTime? time_started { get; set; }
		public DateTime? time_ended { get; set; }
		public int? num_studies_imported { get; set; }
		public int? num_objects_imported { get; set; }
		public int? num_total_studies { get; set; }
		public int? num_total_objects { get; set; }
		public int? num_total_study_object_links { get; set; }
		public string comments { get; set; }

		public AggregationEvent(int _id)
		{
			id = _id;
			time_started = DateTime.Now;
		}

		public AggregationEvent() { }
	}


	public class DataSource
	{
		public int id { get; set; }
		public int? preference_rating { get; set; }
		public string database_name { get; set; }
	}

}
