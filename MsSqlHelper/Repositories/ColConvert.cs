using Microsoft.FSharp.Core;
using System;
using System.Data.SqlTypes;

namespace MsSqlHelper.Repositories
{
    public static class ColConvert
    {
        /// <summary>
        /// Returns false if the supplied value is NULL or DBNull
        /// </summary>
        /// <param name="o"></param>
        /// <returns></returns>
        public static bool ToBool(object o)
        {
            if (o == null || o is DBNull) return false;
            return Convert.ToBoolean(o);
        }

        public static string ToString(object o)
        {
            if (o == null || o is DBNull) return null;
            return Convert.ToString(o);
        }

        public static int ToInt32(object o)
        {
            return Convert.ToInt32(o);
        }

        public static int? ToInt32(object o, int? fallback)
        {
            if (o == null || o is DBNull) return fallback;
            return Convert.ToInt32(o);
        }

        public static long ToInt64(object o)
        {
            return Convert.ToInt64(o);
        }

        public static long? ToInt64(object o, long? fallback)
        {
            if (o == null || o is DBNull) return fallback;
            return Convert.ToInt64(o);
        }

        public static double ToSingle(object o)
        {
            return Convert.ToSingle(o);
        }

        public static double? ToSingle(object o, float? fallback)
        {
            if (o == null || o is DBNull) return fallback;
            return Convert.ToSingle(o);
        }

        public static double ToDouble(object o)
        {
            return Convert.ToDouble(o);
        }

        public static double? ToDouble(object o, double? fallback)
        {
            if (o == null || o is DBNull) return fallback;
            return Convert.ToDouble(o);
        }

        public static decimal ToDecimal(object o)
        {
            return Convert.ToDecimal(o);
        }

        public static decimal? ToDecimal(object o, decimal? fallback)
        {
            if (o == null || o is DBNull) return fallback;
            return Convert.ToDecimal(o);
        }

        public static DateTime ToDateTime(object o)
        {
            return Convert.ToDateTime(o);
        }

        public static DateTimeOffset ToDateTimeOffset(object o)
        {
            return (DateTimeOffset)o;
        }

        public static DateTimeOffset? ToDateTimeOffset(object o, DateTimeOffset? fallback)
        {
            if (o == null || o is DBNull) return fallback;
            return (DateTimeOffset)o;
        }

        public static DateTime? ToDateTime(object o, DateTime? fallback)
        {
            if (o == null || o is DBNull) return fallback;
            return Convert.ToDateTime(o);
        }

        public static DateTime ToSafeSqlDateTime(DateTime dt)
        {
            if (dt.CompareTo(SqlDateTime.MinValue.Value) < 0) return SqlDateTime.MinValue.Value;
            else if (dt.CompareTo(SqlDateTime.MaxValue.Value) > 0) return SqlDateTime.MaxValue.Value;
            else return dt;
        }

        public static DateTimeOffset ToSafeSqlDateTime(DateTimeOffset dt)
        {
            if (dt.CompareTo(SqlDateTime.MinValue.Value) < 0) return SqlDateTime.MinValue.Value;
            else if (dt.CompareTo(SqlDateTime.MaxValue.Value) > 0) return SqlDateTime.MaxValue.Value;
            else return dt;
        }

        public static char ToChar(object o)
        {
            return Convert.ToChar(o);
        }

        public static string ToSparseXmlTag<T>(T sparseObject, string objectName)
        {
            if (sparseObject == null) return string.Empty;

            string parsedObject = sparseObject.ToString();

            if (sparseObject.GetType() == typeof(bool))
            {
                parsedObject = parsedObject == bool.TrueString ? "1" : "0";
            }

            return "<" + objectName + ">" + parsedObject + "</" + objectName + ">";
        }
    }
}
