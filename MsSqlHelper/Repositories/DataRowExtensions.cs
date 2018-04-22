using System;
using System.Data;

namespace MsSqlHelper.Repositories
{
    public static class DataRowExtensions
    {
        public static bool ToBool(this DataRow row, string colname)
        {
            //Convert.ToBoolean returns False if the value supplied is NULL
            return Convert.ToBoolean(row[colname]);
        }

        public static double ToDouble(this DataRow row, string colname)
        {
            return Convert.ToDouble(row[colname]);
        }
        public static long ToLong(this DataRow row, string colname)
        {
            return Convert.ToInt64(row[colname]);
        }
        public static long? ToLong(this DataRow row, string colname, long? d)
        {
            if (!row.IsNull(colname))
                return Convert.ToInt64(row[colname]);
            else return d;
        }

        public static char ToChar(this DataRow row, string colname)
        {
            return Convert.ToChar(row[colname]);
        }
        public static DateTime ToDateTime(this DataRow row, string colname)
        {
            return Convert.ToDateTime(row[colname]);
        }

        public static string GetString(this DataRow row, string colname)
        {
            if (!row.IsNull(colname))
                return Convert.ToString(row[colname]);
            else return null;
        }
    }
}
