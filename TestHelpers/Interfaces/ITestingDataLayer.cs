using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public interface ITestingDataLayer
    {
        void BuildNewADTables();
        void TransferADTableData(ISource source);

    }
}
