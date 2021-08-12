using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public interface ILoggerHelper
    {
        void LogParameters(Options opts);
        void LogHeader(string header_text);
        void LogStudyHeader(Options opts, string dbline);
        //void LogTableStatistics(ISource s, string schema);
    }
}
