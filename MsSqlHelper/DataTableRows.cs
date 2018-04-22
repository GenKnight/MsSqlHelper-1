using MsSqlHelper.Repositories;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MsSqlHelper
{
    public class DataTableRows : IEnumerable<DataTableRows>, IEnumerator<DataTableRows>
    {
        private readonly DataRowCollection _rows;
        private DataRow _row;
        private int _enumerator = -1;

        public DataTableRows(DataRowCollection rows)
        {
            _rows = rows;
            if(_rows.Count > 0)
                _row = _rows[0];
        }

        public bool AsBool(string col)
        {
            return ColConvert.ToBool(_row[col]);
        }

        public string AsString(string col)
        {
            return ColConvert.ToString(_row[col]);
        }

        public int AsInt32(string col)
        {
            return ColConvert.ToInt32(_row[col]);
        }

        public int? AsInt32(string col, int? fallback)
        {
            return ColConvert.ToInt32(_row[col], fallback);
        }

        public long AsInt64(string col)
        {
            return ColConvert.ToInt64(_row[col]);
        }

        public long? AsInt64(string col, long? fallback)
        {
            return ColConvert.ToInt64(_row[col], fallback);
        }

        public double AsSingle(string col)
        {
            return ColConvert.ToSingle(_row[col]);
        }

        public double? AsSingle(string col, float? fallback)
        {
            return ColConvert.ToSingle(_row[col], fallback);
        }

        public double AsDouble(string col)
        {
            return ColConvert.ToDouble(_row[col]);
        }

        public double? AsDouble(string col, double? fallback)
        {
            return ColConvert.ToDouble(_row[col], fallback);
        }

        public decimal AsDecimal(string col)
        {
            return ColConvert.ToDecimal(_row[col]);
        }

        public decimal? AsDecimal(string col, decimal? fallback)
        {
            return ColConvert.ToDecimal(_row[col], fallback);
        }

        public DateTime AsDateTime(string col)
        {
            return ColConvert.ToDateTime(_row[col]);
        }

        public DateTimeOffset AsDateTimeOffset(string col)
        {
            return ColConvert.ToDateTimeOffset(_row[col]);
        }

        public DateTimeOffset? AsDateTimeOffset(string col, DateTimeOffset? fallback)
        {
            return ColConvert.ToDateTimeOffset(_row[col], fallback);
        }

        public DateTime? AsDateTime(string col, DateTime? fallback)
        {
            return ColConvert.ToDateTime(_row[col], fallback);
        }

        public char AsChar(string col)
        {
            return ColConvert.ToChar(_row[col]);
        }


        public bool IsNull(string col)
        {
            return _row[col] == DBNull.Value;
        }

        public IEnumerator<DataTableRows> GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Dispose()
        {

        }

        public bool MoveNext()
        {
            _enumerator++;

            if (_enumerator < _rows.Count)
            {
                _row = _rows[_enumerator];
                return true;
            }

            return false;
        }

        public void Reset()
        {
            _enumerator = -1;
            _row = _rows[0];
        }

        public DataTableRows Current => this;

        object IEnumerator.Current => Current;

    }
}
