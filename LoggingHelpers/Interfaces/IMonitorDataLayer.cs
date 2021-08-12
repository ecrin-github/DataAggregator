using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public interface IMonitorDataLayer
    {
        void SetUpTempContextFTWs(ICredentials credentials);
        void DropTempContextFTWs();

        string SetUpTempFTW(ICredentials credentials, string database_name);
        void DropTempFTW(string database_name);

        Source FetchSourceParameters(int source_id);
        int GetNextAggEventId();
        int GetLastAggEventId();

        int StoreAggregationEvent(AggregationEvent aggregation);
        IEnumerable<Source> RetrieveDataSources();

        void DeleteSameEventDBStats(int agg_event_id);
        int GetRecNum(string table_name, string source_conn_string);
        void DeleteSameEventSummaryStats(int agg_event_id);
        int GetAggregateRecNum(string table_name, string schema_name, string source_conn_string);
        void StoreSourceSummary(SourceSummary sm);
        void StoreAggregationSummary(AggregationSummary asm);
        void DeleteSameEventObjectStats(int agg_event_id);
        List<AggregationObjectNum> GetObjectTypes(int aggregation_event_id);
        void RecreateStudyStudyLinksTable();

        List<StudyStudyLinkData> GetStudyStudyLinkData(int aggregation_event_id);
        List<StudyStudyLinkData> GetStudyStudyLinkData2(int aggregation_event_id);

        ulong StoreObjectNumbers(PostgreSQLCopyHelper<AggregationObjectNum> copyHelper,
                                         IEnumerable<AggregationObjectNum> entities);
        ulong StoreStudyLinkNumbers(PostgreSQLCopyHelper<StudyStudyLinkData> copyHelper,
                                        IEnumerable<StudyStudyLinkData> entities);
        
    }
}
