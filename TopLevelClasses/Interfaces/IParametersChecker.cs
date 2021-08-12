using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    internal interface IParametersChecker
    {
        Options ObtainParsedArguments(string[] args);
        bool ValidArgumentValues(Options opts);
    }
}
