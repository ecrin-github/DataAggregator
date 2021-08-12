using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public interface IAggregator
    {
        int AggregateData(Options opts);
    }
}
