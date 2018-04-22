using System;
using System.Collections.Generic;
using Montel.Databus.Messages.FundamentalData.Entities;
using Microsoft.FSharp.Core;
using System.Spatial;

namespace MsSqlHelperConsoleTester.TestClasses
{
    class FundamentalDataGenerator
    {
        public List<FundamentalProduct> GenerateProductSeries(int amount = 1, int expectedElements = 1)
        {
            var returnList = new List<FundamentalProduct>();

            for (int i = 0; i < amount; i++)
            {
                returnList.Add(GenerateProduct(i, expectedElements));
            }
            return returnList;

        }

        public List<FundamentalValue> GeneraterValueSeries(FundamentalProduct product)
        {
            var random = new Random();
            var returnList = new List<FundamentalValue>();

            for (int i = 0; i < product.ExpectedElementsPerDay.Value; i++)
            {
                returnList.Add(GenerateValue(product.ProductCode, i, random));
            }

            return returnList;
        }

        private FundamentalProduct GenerateProduct(int productCode, int expectedElements)
        {
            var DataSource = "OddSource";
            var RequiredServiceID = 24;
            var ProductCode = DataSource + "_" + productCode;
            var ProductDescription = "desc_" + Guid.NewGuid().ToString();
            var ExpectedElementsPerDay = expectedElements;
            var IsPublished = false;
            var RecTime = DateTime.Now;
            var SQLTimeZoneName = "Central Europe Standard Time";
            var IANATimeZoneName = "Europe/Oslo";

            var ElementGroup = "Weather";
            var ProductSource = "Odd";
            var ProductCountry = "Norway";
            var ProductZone = "50 Hertz";
            var ProductZoneTo = "60 Hertz";
            var ProductUnitLong = "Megawatt";
            var ProductUnitShort = "MW";
            var IntervalType = "Day";
            var IntervalValue = 1;
            var Forecast = false;
            var ElementType = "Wind";
            var ProductTimeZone = "CET";
            var ProductTimeZoneOffset = "+01:00";
            var ContentRange = "Current Day";
            var PowerSource = "Heart";
            var GasStorageLimit = 0;
            var Color = "Red";
            var PowerCompanyName = "OddPower!";
            var PowerPlantName = "OddPlant01";
            var PowerPlantUnitName = "01";
            var ContentType = "Unrealistic";
            var ExpectedElementName = "Test";
            var InstalledCapacity = 10;
            var AvailableCapacity = 1;
            var StationID = "02";
            var StationName = "Backup";

            SparseProductDetails detailSet = new SparseProductDetails(
                elementGroup: FSharpOption<string>.Some(ElementGroup),
                productSource: FSharpOption<string>.Some(ProductSource),
                productCountry: FSharpOption<string>.Some(ProductCountry),
                productZone: FSharpOption<string>.Some(ProductZone),
                productZoneTo: FSharpOption<string>.Some(ProductZoneTo),
                productUnitLong: FSharpOption<string>.Some(ProductUnitLong),
                productUnitShort: FSharpOption<string>.Some(ProductUnitShort),
                intervalType: FSharpOption<string>.Some(IntervalType),
                intervalValue: FSharpOption<int>.Some(IntervalValue),
                forecast: FSharpOption<bool>.Some(Forecast),
                elementType: FSharpOption<string>.Some(ElementType),
                productTimeZone: FSharpOption<string>.Some(ProductTimeZone),
                productTimeZoneOffset: FSharpOption<string>.Some(ProductTimeZoneOffset),
                contentRange: FSharpOption<string>.Some(ContentRange),
                powerSource: FSharpOption<string>.Some(PowerSource),
                gasStorageLimit: FSharpOption<int>.Some(GasStorageLimit),
                color: FSharpOption<string>.Some(Color),
                powerCompanyName: FSharpOption<string>.Some(PowerCompanyName),
                powerPlantName: FSharpOption<string>.Some(PowerPlantName),
                powerPlantUnitName: FSharpOption<string>.Some(PowerPlantUnitName),
                contentType: FSharpOption<string>.Some(ContentType),
                expectedElementNames: FSharpOption<string>.Some(ExpectedElementName),
                installedCapacity: FSharpOption<int>.Some(InstalledCapacity),
                availableCapacity: FSharpOption<int>.Some(AvailableCapacity),
                stationID: FSharpOption<string>.Some(StationID),
                stationName: FSharpOption<string>.Some(StationName)
                );


            return new FundamentalProduct(
                dataSource: DataSource,
                sourceCode: FSharpOption<string>.None,
                requiredServiceID: FSharpOption<int>.Some(RequiredServiceID),
                productCode: ProductCode,
                parentCode: FSharpOption<string>.None,
                productDescription: ProductDescription,
                expectedElementsPerDay: FSharpOption <int>.Some(ExpectedElementsPerDay),
                isPublished: IsPublished,
                mapData: FSharpOption<Geography>.None,
                recTime: RecTime,
                sQLTimeZoneName: FSharpOption<string>.Some(SQLTimeZoneName),
                iANATimeZoneName: FSharpOption<string>.Some(IANATimeZoneName),
                detailSet: detailSet
                );

        }

        private FundamentalValue GenerateValue(string productCode, int elementNo, Random randomObject)
        {
            var AsOfDate = DateTime.Now.Date;
            var DateTimeFrom = DateTime.Now.AddHours(elementNo);
            var DateTimeTo = DateTime.Now.AddDays(1).AddHours(elementNo);
            decimal Value = (decimal)randomObject.NextDouble();
            var RecTime = DateTime.Now;

            return new FundamentalValue(
                productCode: productCode,
                asOfDate: AsOfDate,
                dateTimeFrom: DateTimeFrom,
                dateTimeTo: DateTimeTo,
                value: Value,
                recTime: RecTime
                );
        }
    }
}
