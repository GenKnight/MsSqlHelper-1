using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Spatial;

namespace MsSqlHelper
{
    public class SqlRecordWriter
    {
        private readonly SqlMetaData[] _metadata;
        private readonly Dictionary<string, int> _colmap = new Dictionary<string, int>();

        public SqlRecordWriter(SqlMetaData[] metadata)
        {
            _metadata = metadata;
            for (int i = 0; i < metadata.Length; ++i)
                _colmap.Add(metadata[i].Name, i);
            Record = new SqlDataRecord(_metadata);
        }

        public SqlDataRecord GetRecord(bool forceNew = false)
        {
            if (forceNew)
                Record = new SqlDataRecord(_metadata);

            return Record;
        }

        private SqlDataRecord Record { get; set; }

        public void SetString(string colName, string s)
        {
            if (s == null)
                Record.SetDBNull(_colmap[colName]);
            else
                Record.SetString(_colmap[colName], s);
        }

        public void SetInt32(string colName, int? v)
        {
            if (v.HasValue)
                Record.SetInt32(_colmap[colName], v.Value);
            else
                Record.SetDBNull(_colmap[colName]);
        }

        public void SetInt64(string colName, long? v)
        {
            if (v.HasValue)
                Record.SetInt64(_colmap[colName], v.Value);
            else
                Record.SetDBNull(_colmap[colName]);
        }

        public void SetDateTimeOffset(string colname, DateTimeOffset? d)
        {
            if (d.HasValue)
                Record.SetDateTimeOffset(_colmap[colname], d.Value);
            else
                Record.SetDBNull(_colmap[colname]);
        }
        public void SetDateTime(string colname, DateTime? d)
        {
            if (d.HasValue)
                Record.SetDateTime(_colmap[colname], d.Value);
            else
                Record.SetDBNull(_colmap[colname]);
        }

        public void SetBoolean(string colname, bool? preArranged)
        {
            if (preArranged.HasValue)
                Record.SetBoolean(_colmap[colname], preArranged.Value);
            else
                Record.SetDBNull(_colmap[colname]);
        }

        public void SetMoney(string colname, decimal? d)
        {
            if (d.HasValue)
                Record.SetSqlMoney(_colmap[colname], d.Value);
            else
                Record.SetDBNull(_colmap[colname]);
        }

        public void SetMoney(string colname, double? d)
        {
            if (d.HasValue)
                Record.SetDecimal(_colmap[colname], Convert.ToDecimal(d.Value));
            else
                Record.SetDBNull(_colmap[colname]);
        }

        public void SetDecimal(string colname, double? d)
        {
            if (d.HasValue)
                Record.SetDecimal(_colmap[colname], Convert.ToDecimal(d.Value));
            else
                Record.SetDBNull(_colmap[colname]);
        }

        public void SetDecimal(string colname, decimal? d)
        {
            if (d.HasValue)
                Record.SetDecimal(_colmap[colname], d.Value);
            else
                Record.SetDBNull(_colmap[colname]);
        }

        public void SetChar(string colname, char? d)
        {
            if (d.HasValue)
                Record.SetString(_colmap[colname], d.Value.ToString());
            else
                Record.SetDBNull(_colmap[colname]);
        }


        public void SetGeography(string colname, Geography g)
        {
            if (g == null)
                Record.SetDBNull(_colmap[colname]);
            else
                Record.SetValue(_colmap[colname], g);
        }
    }
}
