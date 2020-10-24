using Dapper.Contrib.Extensions;
using System;

namespace DataAggregator
{
    [Table("sf.source_parameters")]
    public class Source
    {
        public int id { get; set; }
        public int? preference_rating { get; set; }
        public string database_name { get; set; }
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


    [Table("sf.source_summaries")]
    public class SourceSummary	
    {
        [Key]
        public int id { get; set; }
        public int aggregation_event_id { get; set; }
        public DateTime agregation_datetime { get; set; }
        public string database_name { get; set; }
        public int study_recs { get; set; }
        public int study_identifiers_recs { get; set; }
        public int study_titles_recs { get; set; }
        public int study_contributors_recs { get; set; }
        public int study_topics_recs { get; set; }
        public int study_features_recs { get; set; }
        public int study_references_recs { get; set; }
        public int study_relationships_recs { get; set; }
        public int data_object_recs { get; set; }
        public int object_datasets_recs { get; set; }
        public int object_instances_recs { get; set; }
        public int object_titles_recs { get; set; }
        public int object_dates_recs { get; set; }
        public int object_contributors_recs { get; set; }
        public int object_topics_recs { get; set; }
        public int object_identifiers_recs { get; set; }
        public int object_descriptions_recs { get; set; }
        public int object_rights_recs { get; set; }
        public int object_relationships_recs { get; set; }

        public SourceSummary(int _aggregation_event_id, string _database_name)
        {
            aggregation_event_id = _aggregation_event_id;
            agregation_datetime = DateTime.Now;
            database_name = _database_name;
        }
    }

    [Table("sf.aggregation_summaries")]
    public class AggregationSummary
    {
        [Key]
        public int id { get; set; }
        public int aggregation_event_id { get; set; }
        public DateTime agregation_datetime { get; set; }
        public int study_recs { get; set; }
        public int study_identifiers_recs { get; set; }
        public int study_titles_recs { get; set; }
        public int study_contributors_recs { get; set; }
        public int study_topics_recs { get; set; }
        public int study_features_recs { get; set; }
        public int study_relationships_recs { get; set; }

        public int data_object_recs { get; set; }
        public int object_datasets_recs { get; set; }
        public int object_instances_recs { get; set; }
        public int object_titles_recs { get; set; }
        public int object_dates_recs { get; set; }
        public int object_contributors_recs { get; set; }
        public int object_topics_recs { get; set; }
        public int object_identifiers_recs { get; set; }
        public int object_descriptions_recs { get; set; }
        public int object_rights_recs { get; set; }
        public int object_relationships_recs { get; set; }

        public int study_object_link_recs { get; set; }

        public AggregationSummary(int _aggregation_event_id)
        {
            aggregation_event_id = _aggregation_event_id;
            agregation_datetime = DateTime.Now;
        }
    }

    [Table("sf.aggregation_object_numbers")]
    public class AggregationObjectNum
    {
        [Key]
        public int id { get; set; }
        public int aggregation_event_id { get; set; }
        public int object_type_id { get; set; }
        public string object_type_name { get; set; }
        public int number_of_type { get; set; }
    }


    [Table("sf.study_study_link_data")]
    public class StudyStudyLinkData
    {
        [Key]
        public int id { get; set; }
        public int source_id { get; set; }
        public string source_name { get; set; }
        public int other_source_id { get; set; }
        public string other_source_name { get; set; }
        public int number_in_other_source { get; set; }
    }


    [Table("sf.aggregation_events")]
    public class AggregationEvent
    {
        [ExplicitKey]
        public int id { get; set; }
        public DateTime? time_started { get; set; }
        public DateTime? time_ended { get; set; }
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

        public DataSource(int _id, int? _preference_rating, string _database_name)
        {
            id = _id;
            preference_rating = _preference_rating;
            database_name = _database_name;

        }
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


}
