using Nito.AsyncEx;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using MsSqlHelperConsoleTester.TestClasses;
using MsSqlHelper;
using Montel.Databus.Messages.FundamentalData.Entities;

namespace MsSqlHelperConsoleTester
{
    class Program
    {
        static void Main(string[] args)
        {
            AsyncContext.Run(() => MainAsync(args));
        }

        static async Task MainAsync(string[] args)
        {
            string connectionString = ConfigurationManager.ConnectionStrings["DBConnectionString"].ConnectionString;
            var tester = new SqlTester(connectionString);
            var templates = new List<RequestTemplate>();
            var fundaGenerator = new FundamentalDataGenerator();

            var areas = new List<DomainArea>();
            var values = new List<FundamentalValue>();
            var existingValues = new List<DataTableRows>();
            var products = new List<FundamentalProduct>();

            products.AddRange(fundaGenerator.GenerateProductSeries(10, 24));
            products.ForEach(p => {
                tester.StoreProduct(p);
                values.AddRange(fundaGenerator.GeneraterValueSeries(p));
                tester.StoreValues(values);
                });


            existingValues.AddRange(await tester.GetLatestValuesByProductCodeAsync("ENTSO-E_A65_A16_10Y1001A1001A46L", new DateTimeOffset(2017, 02, 20, 23, 00, 00, TimeSpan.FromHours(1)), new DateTimeOffset(2018, 02, 20, 23, 00, 00, TimeSpan.FromHours(1))));
            var test = existingValues.Select(v => v.AsInt32("Value")).ToList();

            //for (int i = 0; i < 500; i++)
            //{
            //    templates.AddRange(await tester.GetAllTemplatesAsync());
            //    areas.AddRange(await tester.GetAllDomainAreasAsync());
            //}

            Console.WriteLine("All results done");
            Console.ReadLine();
        }
    }
}
