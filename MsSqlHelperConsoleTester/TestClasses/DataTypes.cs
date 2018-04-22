using Microsoft.SqlServer.Server;
using System.Data;

namespace MsSqlHelperConsoleTester.TestClasses
{
    class DataTypes
    {
        public static SqlMetaData[] ValueDTO
        {
            get
            {
                var schema = new[]
{
                new SqlMetaData("ProductCode", SqlDbType.NVarChar, 50),
                new SqlMetaData("AsOfDate", SqlDbType.DateTime),
                new SqlMetaData("DateTimeFrom", SqlDbType.DateTimeOffset),
                new SqlMetaData("DateTimeTo", SqlDbType.DateTimeOffset),
                new SqlMetaData("Value", SqlDbType.Decimal)
                };

                return schema;
            }
        }
    }
}
