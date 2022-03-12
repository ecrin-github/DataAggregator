using System.Collections.Generic;
using Serilog;

namespace DataAggregator
{
    public class CoreTransferBuilder
    {
        string _connString;
        ILogger _logger;
        CoreTransferHelper core_tr;

        public CoreTransferBuilder(string connString, ILogger logger)
        {
            _logger = logger;
            _connString = connString;
            core_tr = new CoreTransferHelper(_connString, _logger);
        }

        public void TransferCoreStudyData()
        {
            int res;
            res = core_tr.LoadCoreStudyData();
            _logger.Information(res.ToString() + " core studies transferred");
            res = core_tr.LoadCoreStudyIdentifiers();
            _logger.Information(res.ToString() + " core study identifiers transferred");
            res = core_tr.LoadCoreStudyTitles();
            _logger.Information(res.ToString() + " core study titles transferred");
            res = core_tr.LoadCoreStudyContributors();
            _logger.Information(res.ToString() + " core study contributors transferred");
            res = core_tr.LoadCoreStudyTopics();
            _logger.Information(res.ToString() + " core study topics transferred");
            res = core_tr.LoadCoreStudyFeatures();
            _logger.Information(res.ToString() + " core study features transferred");
            res = core_tr.LoadCoreStudyRelationShips();
            _logger.Information(res.ToString() + " core study relationships transferred");
        }


        public void TransferCoreObjectData()
        {
            int res;
            res = core_tr.LoadCoreDataObjects();
            _logger.Information(res.ToString() + " core data objects transferred");
            res = core_tr.LoadCoreObjectDatasets();
            _logger.Information(res.ToString() + " core object datasets transferred");
            res = core_tr.LoadCoreObjectInstances();
            _logger.Information(res.ToString() + " core object instances transferred");
            res = core_tr.LoadCoreObjectTitles();
            _logger.Information(res.ToString() + " core object titles transferred");
            res = core_tr.LoadCoreObjectDates();
            _logger.Information(res.ToString() + " core object dates transferred");
            res = core_tr.LoadCoreObjectContributors();
            _logger.Information(res.ToString() + " core object contributors transferred");
            res = core_tr.LoadCoreObjectTopics();
            _logger.Information(res.ToString() + " core object topics transferred");
            res = core_tr.LoadCoreObjectDescriptions();
            _logger.Information(res.ToString() + " core object descriptions transferred");
            res = core_tr.LoadCoreObjectIdentifiers();
            _logger.Information(res.ToString() + " core object identifiers transferred");
            res = core_tr.LoadCoreObjectRelationships();
            _logger.Information(res.ToString() + " core object relationships transferred");
            res = core_tr.LoadCoreObjectRights();
            _logger.Information(res.ToString() + " core object rights transferred");
        }

        public void TransferCoreLinkData()
        {
            int res = core_tr.LoadStudyObjectLinks();
            _logger.Information(res.ToString() + " core link data transferred");
        }


        public void GenerateProvenanceData()
        {
            core_tr.GenerateStudyProvenanceData();
            _logger.Information("Core study provenance data created");
            core_tr.GenerateObjectProvenanceData();
            _logger.Information("Core object provenance data created");
        }

    }
}
