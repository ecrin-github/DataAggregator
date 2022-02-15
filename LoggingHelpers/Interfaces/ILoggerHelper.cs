using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public interface ILoggerHelper
    {
        void LogParameters(Options opts);
        void LogHeader(string header_text);
        void SpacedInformation(string header_text);
        void LogStudyHeader(bool using_test_data, string dbline);
        //void LogTableStatistics(ISource s, string schema);
    }
}
