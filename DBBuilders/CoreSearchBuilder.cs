using System.Collections.Generic;
using Serilog;

namespace DataAggregator
{
    public class CoreSearchBuilder
    {
        string _connString;
        ILogger _logger;
        CoreSearchHelper core_srch;

        public CoreSearchBuilder(string connString, ILogger logger)
        {
            _logger = logger;
            _connString = connString;
            core_srch = new CoreSearchHelper(_connString, _logger);
        }

        public void CreateStudyFeatureSearchData()
        {
            // Set up

            int res;
            core_srch.SetupSearchMinMaxes(); // set up parameters for calls
            res = core_srch.GenerateStudySearchData();
            _logger.Information(res.ToString() + " study search records created");


            res = core_srch.UpdateStudySearchDataWithPhaseData();
            _logger.Information(res.ToString() + " study search records updated with phase data");
            res = core_srch.UpdateStudySearchDataWithPurposeData();
            _logger.Information(res.ToString() + " study search records updated with purpose data");
            res = core_srch.UpdateStudySearchDataWithAllocationData();
            _logger.Information(res.ToString() + " study search records updated with allocation data");
            res = core_srch.UpdateStudySearchDataWithInterventionData();
            _logger.Information(res.ToString() + " study search records updated with intervention data");
            res = core_srch.UpdateStudySearchDataWithMaskingData();
            _logger.Information(res.ToString() + " study search records updated with masking data");
            res = core_srch.UpdateStudySearchDataWithObsModelData();
            _logger.Information(res.ToString() + " study search records updated with observational model data");
            res = core_srch.UpdateStudySearchDataWithTimePerspData();
            _logger.Information(res.ToString() + " study search records updated with time perspective data");
            res = core_srch.UpdateStudySearchDataWithBioSpecData();
            _logger.Information(res.ToString() + " study search records updated with biospecimen data");

        }

        public void CreateStudyObjectSearchData()
        {
            int res;
            core_srch.CreateStudySearchObjectDataTable();
            res = core_srch.UpdateStudySearchDataIfHasRegEntry();
            _logger.Information(res.ToString() + " study search records updated with has reg entry data");
            res = core_srch.UpdateStudySearchDataIfHasRegResults();
            _logger.Information(res.ToString() + " study search records updated with has reg results data");
            res = core_srch.UpdateStudySearchDataIfHasArticle();
            _logger.Information(res.ToString() + " study search records updated with has article data");
            res = core_srch.UpdateStudySearchDataIfHasProtocol();
            _logger.Information(res.ToString() + " study search records updated with has protocol data");
            res = core_srch.UpdateStudySearchDataIfHasOverview();
            _logger.Information(res.ToString() + " study search records updated with has overview data");
            res = core_srch.UpdateStudySearchDataIfHasPIF();
            _logger.Information(res.ToString() + " study search records updated with has PIF data");
            res = core_srch.UpdateStudySearchDataIfHasECRFs();
            _logger.Information(res.ToString() + " study search records updated with has eCRFs data");
            res = core_srch.UpdateStudySearchDataIfHasManual();
            _logger.Information(res.ToString() + " study search records updated with has manual data");
            res = core_srch.UpdateStudySearchDataIfHasSAP();
            _logger.Information(res.ToString() + " study search records updated with has SAP data");
            res = core_srch.UpdateStudySearchDataIfHasCSR();
            _logger.Information(res.ToString() + " study search records updated with has CSR data");
            res = core_srch.UpdateStudySearchDataIfHasDataDesc();
            _logger.Information(res.ToString() + " study search records updated with has data description data");
            res = core_srch.UpdateStudySearchDataIfHasIPD();
            _logger.Information(res.ToString() + " study search records updated with has IPD data");
            res = core_srch.UpdateStudySearchDataIfHasAggData();
            _logger.Information(res.ToString() + " study search records updated with has aggregate data");
            res = core_srch.UpdateStudySearchDataIfHasOthRes();
            _logger.Information(res.ToString() + " study search records updated with has other resource data");
            res = core_srch.UpdateStudySearchDataIfHasConfMat();
            _logger.Information(res.ToString() + " study search records updated with has conference material data");
            res = core_srch.UpdateStudySearchDataIfHasOthArt();
            _logger.Information(res.ToString() + " study search records updated with has other article data");
            res = core_srch.UpdateStudySearchDataIfHasChapter();
            _logger.Information(res.ToString() + " study search records updated with has chapter data");
            res = core_srch.UpdateStudySearchDataIfHasOthInfo();
            _logger.Information(res.ToString() + " study search records updated with has other info data");
            res = core_srch.UpdateStudySearchDataIfHasWebSite();
            _logger.Information(res.ToString() + " study search records updated with has web site data");
            res = core_srch.UpdateStudySearchDataIfHasSoftware();
            _logger.Information(res.ToString() + " study search records updated with has software data");
            res = core_srch.UpdateStudySearchDataIfHasOther();
            _logger.Information(res.ToString() + " study search records updated with has other data");
            core_srch.DropStudySearchObjectDataTable();

        }

        public void CreateStudyTextSearchData()
        {
            int res;            
            
            // set up

            core_srch.SetupSearchMinMaxes(); // set up parameters for calls
            core_srch.CreateTSConfig1(); // ensure test search configs up to date
            core_srch.CreateTSConfig2();

            // For both titles and topics, set up temporary tables
            // do an initial transition to lexemes and then aggregate to
            // study based text, before indexing

            // titles

            res = core_srch.GenerateTempTitleData();
            _logger.Information(res.ToString() + " temporary title records created");

            res = core_srch.GenerateTitleLexemeStrings();
            _logger.Information(res.ToString() + " title lexeme records created");

            res = core_srch.GenerateTitleDataByStudy();
            _logger.Information(res.ToString() + " title lexeme records, by study, created");

            res = core_srch.TransferTitleDataByStudy();
            _logger.Information(res.ToString() + " records created");

            core_srch.IndexTitleText();
            _logger.Information("title lexeme indices created");

            // tgopics

            res = core_srch.GenerateTopicData();
            _logger.Information(res.ToString() + " temporary topic records created");

            res = core_srch.GenerateTopicLexemeStrings();
            _logger.Information(res.ToString() + " title lexeme records created");

            res = core_srch.GenerateTopicDataByStudy();
            _logger.Information(res.ToString() + " topic lexeme records, by study, created");

            res = core_srch.TransferTopicDataByStudy();
            _logger.Information(res.ToString() + " topic lexeme records created");

            core_srch.IndexTopicText();
            _logger.Information("topic lexeme indices created");

            // tidy up

            core_srch.DropTempSearchTables();
        }

    }
}
