using Montel.Databus.Messages.FundamentalData.Entities;
using MsSqlHelper;
using MsSqlHelper.Repositories;
using MsSqlHelperConsoleTester.TestClasses;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.FSharp.Core;
using System.Spatial;

namespace MsSqlHelperConsoleTester
{
    public class SqlTester : BaseSqlRepository
    {
        public SqlTester(string connectionString, IsolationLevel? isolationLevel) : base(connectionString, isolationLevel)
        {

        }

        public SqlTester(string connectionString) : base(connectionString)
        {

        }

        public async Task<IEnumerable<RequestTemplate>> GetAllTemplatesAsync()
        {
            try
            {
                var result = await ExecuteProcedureOrSqlAsync("[Montel].[ENTSOE].[GetAllTemplates]", CommandType.StoredProcedure);

                var templates = result.Select(GetTemplateFromDataRow).ToList();

                return templates;
            }
            catch (Exception ex)
            {
                throw new Exception("Could not get Templates", ex);
            }
        }

        public async Task<IEnumerable<DomainArea>> GetAllDomainAreasAsync()
        {
            try
            {
                var result = await ExecuteProcedureOrSqlAsync("[Montel].[ENTSOE].[GetAllAreas]", CommandType.StoredProcedure);

                var areas = result.Select(GetDomainAreasFromDataRow).ToList();

                return areas;
            }
            catch (Exception ex)
            {
                throw new Exception("Could not get Areas", ex);
            }
        }

        public async Task<DataTableRows> GetLatestValuesByProductCodeAsync(string productCode, DateTimeOffset dateTimeFrom, DateTimeOffset dateTimeTo)
        {
            try
            {
                var parameters = new[] {
                    new SqlParameter("ProductCode", productCode),
                    new SqlParameter("TimeFrom", dateTimeFrom),
                    new SqlParameter("TimeTo", dateTimeTo)
                };

                return await ExecuteProcedureOrSqlAsync("[Montel].[Fundamental].[GetLatestValuesByProductCode]", CommandType.StoredProcedure, parameters);
            }
            catch (Exception ex)
            {
                throw new Exception("Could not get Fundamental Values.", ex);
            }
        }

        private RequestTemplate GetTemplateFromDataRow(DataTableRows row)
        {
            try
            {
                var t = new RequestTemplate();
                t.TemplateID = row.AsInt32("RequestTemplateID");
                t.Description = row.AsString("RequestDescription");
                t.DocumentType = row.AsString("DocumentType");
                t.DocStatus = row.AsString("DocStatus");
                t.ProcessType = row.AsString("ProcessType");
                t.BusinessType = row.AsString("BusinessType");
                t.PsrType = row.AsString("PsrType");
                t.TypeMarketAgreementType = row.AsString("TypeMarketAgreementType");
                t.ContractMarketAgreementType = row.AsString("ContractMarketAgreementType");
                t.AuctionType = row.AsString("AuctionType");
                t.AuctionCategory = row.AsString("AuctionCategory");
                t.ClassificationSequenceAttributeInstanceComponent = row.AsString("ClassificationSequenceAttributeInstanceComponent");

                if (row.AsBool("OutBiddingZoneDomain")) t.UsedDomains |= RequestTemplate.DomainsInUse.OutBiddingZoneDomain;
                if (row.AsBool("BiddingZoneDomain")) t.UsedDomains |= RequestTemplate.DomainsInUse.BiddingZoneDomain;
                if (row.AsBool("InDomain")) t.UsedDomains |= RequestTemplate.DomainsInUse.InDomain;
                if (row.AsBool("OutDomain")) t.UsedDomains |= RequestTemplate.DomainsInUse.OutDomain;
                if (row.AsBool("AcquiringDomain")) t.UsedDomains |= RequestTemplate.DomainsInUse.AcquiringDomain;
                if (row.AsBool("ConnectingDomain")) t.UsedDomains |= RequestTemplate.DomainsInUse.ConnectingDomain;
                if (row.AsBool("ControlAreaDomain")) t.UsedDomains |= RequestTemplate.DomainsInUse.ControlAreaDomain;

                t.InDomainAndOutDomainMustMatch = row.AsBool("InDomainAndOutDomainMustMatch");

                if (row.AsBool("TimeInterval")) t.UsedTimePeriods |= RequestTemplate.TimePeriodsInUse.TimeInterval;
                if (row.AsBool("TimeIntervalUpdate")) t.UsedTimePeriods |= RequestTemplate.TimePeriodsInUse.TimeIntervalUpdate;
                if (row.AsBool("PeriodStart")) t.UsedTimePeriods |= RequestTemplate.TimePeriodsInUse.PeriodStart;
                if (row.AsBool("PeriodEnd")) t.UsedTimePeriods |= RequestTemplate.TimePeriodsInUse.PeriodEnd;
                if (row.AsBool("PeriodStartUpdate")) t.UsedTimePeriods |= RequestTemplate.TimePeriodsInUse.TimeInterval;
                if (row.AsBool("PeriodEndUpdate")) t.UsedTimePeriods |= RequestTemplate.TimePeriodsInUse.TimeInterval;

                return t;
            }
            catch (Exception ex)
            {
                throw new Exception("Exception occurred while converting template to an object.", ex);
            }
        }


        private DomainArea GetDomainAreasFromDataRow(DataTableRows row)
        {
            try
            {
                var area = new DomainArea();

                area.AreaCode = row.AsString("Code");
                area.Description = row.AsString("Meaning");

                return area;
            }
            catch (Exception ex)
            {
                throw new Exception("Exception occurred while converting area to an object.", ex);
            }
        }

        public void StoreProduct(FundamentalProduct product)
        {
            try
            {
                ExecuteNonQuery("[Montel].[Fundamental].[InsertOrUpdateProduct]", CommandType.StoredProcedure, CreateProductParameters(product));
            }
            catch (Exception ex)
            {
                var foo = ex;
                throw;
            }
        }

        public virtual void StoreValues(ICollection<FundamentalValue> values)
        {
            try
            {
                var structuredValues = CreateValueParameters(values.ToList());
                ExecuteNonQuery("[Montel].[Fundamental].[InsertOrUpdateValuesFromFeed]", CommandType.StoredProcedure,
                    new SqlParameter("Input", SqlDbType.Structured) { Value = structuredValues.Select(v => v.GetRecord()) });

            }
            catch (Exception ex)
            {
                var foo = ex;
                throw;
            }
        }

        private SqlParameter[] CreateProductParameters(FundamentalProduct product)
        {
            string sparseXml = "";

            //Creates sparse xml tags for all properties in the DetailSet that are not null.
            foreach (var property in product.DetailSet.GetType().GetProperties())
            {
                //This is a hack, a dirty dirty hack, but it works!
                //To clarify: Gets the value from an F# Option-type if it is passed. Reflection within a reflection, refleception!
                if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(FSharpOption<>))
                {
                    
                    object optionObj = (property.GetValue(product.DetailSet));
                    Type objType = optionObj.GetType();
                    object value = objType.GetProperty("Value").GetValue(optionObj);

                    sparseXml += ColConvert.ToSparseXmlTag(value, property.Name);
                }
                else
                {
                    sparseXml += ColConvert.ToSparseXmlTag(property.GetValue(product.DetailSet), property.Name);
                }
                
            }

            var parameters = new[] {
                new SqlParameter("@DataSource", product.DataSource),
                new SqlParameter("@SourceCode", FSharpOption<string>.get_IsSome(product.SourceCode) ? product.SourceCode.Value : null),
                new SqlParameter("@RequiredServiceID", FSharpOption<int>.get_IsSome(product.RequiredServiceID) ? product.RequiredServiceID.Value : (int?) null),
                new SqlParameter("@ProductCode", product.ProductCode),
                new SqlParameter("@ParentCode", FSharpOption<string>.get_IsSome(product.ParentCode) ? product.ParentCode.Value : null),
                new SqlParameter("@ProductDescription", product.ProductDescription),
                new SqlParameter("@ExpectedElementsPerDay", product.ExpectedElementsPerDay),
                new SqlParameter("@IsPublished", product.IsPublished),
                new SqlParameter("@RecTime", product.RecTime.DateTime),
                new SqlParameter("@MapData", FSharpOption<Geography>.get_IsSome(product.MapData) ? product.MapData.Value : null),
                new SqlParameter("@SQLTimeZoneName", FSharpOption<string>.get_IsSome(product.SQLTimeZoneName) ? product.SQLTimeZoneName.Value : null),

                //The following parameters are sparse.
                //Therefore they are represented as a XML string.
                new SqlParameter("@DetailSet", sparseXml)
            };

            return parameters;
        }

        private IEnumerable<SqlRecordWriter> CreateValueParameters(List<FundamentalValue> values)
        {
            var record = new SqlRecordWriter(DataTypes.ValueDTO);

            var records = values.Select(val =>
            {
                record.SetString("ProductCode", val.ProductCode);
                record.SetDateTime("AsOfDate", val.AsOfDate);
                record.SetDateTimeOffset("DateTimeFrom", val.DateTimeFrom);
                record.SetDateTimeOffset("DateTimeTo", val.DateTimeTo);
                record.SetDecimal("Value", val.Value);

                return record;
            });

            return records;
        }
    }
}
