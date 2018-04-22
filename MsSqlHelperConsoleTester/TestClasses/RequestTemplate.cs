using System;
using System.Collections.Generic;

namespace MsSqlHelperConsoleTester.TestClasses
{
    /// <summary>
    /// Temporart copy/paste for testing with a known stored procedure.
    /// </summary>
    public class RequestTemplate
    {
        [Flags]
        public enum DomainsInUse
        {
            ControlAreaDomain = 1,
            OutBiddingZoneDomain = 2,
            BiddingZoneDomain = 4,
            InDomain = 8,
            OutDomain = 16,
            AcquiringDomain = 32,
            ConnectingDomain = 64
        }

        [Flags]
        public enum TimePeriodsInUse
        {
            TimeInterval = 1,
            TimeIntervalUpdate = 2,
            PeriodStart = 4,
            PeriodEnd = 8,
            PeriodStartUpdate = 16,
            PeriodEndUpdate = 32
        }
        public int TemplateID { get; set; }
        public string Description { get; set; }
        public string DocumentType { get; set; }
        public string DocStatus { get; set; }
        public string ProcessType { get; set; }
        public string BusinessType { get; set; }
        public string PsrType { get; set; }
        public string TypeMarketAgreementType { get; set; }
        public string ContractMarketAgreementType { get; set; }
        public string AuctionType { get; set; }
        public string AuctionCategory { get; set; }
        public string ClassificationSequenceAttributeInstanceComponent { get; set; }

        public DomainsInUse UsedDomains { get; set; }

        public TimePeriodsInUse UsedTimePeriods { get; set; }

        public bool InDomainAndOutDomainMustMatch { get; set; }

        public IEnumerable<string> GetTypeCodes()
        {
            var returnList = new List<string>();

            if (DocumentType != null) returnList.Add(DocumentType);
            if (DocStatus != null) returnList.Add(DocStatus);
            if (ProcessType != null) returnList.Add(ProcessType);
            if (BusinessType != null) returnList.Add(BusinessType);
            if (PsrType != null) returnList.Add(PsrType);
            if (TypeMarketAgreementType != null) returnList.Add(TypeMarketAgreementType);
            if (ContractMarketAgreementType != null) returnList.Add(ContractMarketAgreementType);
            if (AuctionType != null) returnList.Add(AuctionType);
            if (AuctionCategory != null) returnList.Add(AuctionCategory);

            return returnList;
        }
    }
}
