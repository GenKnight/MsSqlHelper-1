using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace MsSqlHelper
{
    public class DataSetReader : IDisposable
    {
        private readonly SqlCommand _cmd;

        public DataSetReader(SqlCommand cmd)
        {
            _cmd = cmd;
        }

        public void Dispose()
        {
            _cmd.Dispose();
        }


        public DataSet ExportToDataSet()
        {
            using (var adapter = new SqlDataAdapter(_cmd))
            {
                var ds = new DataSet();
                adapter.Fill(ds);
                adapter.Dispose();

                return ds;
            }
        }

        public async Task<DataSet> ExportToDataSetAsync()
        {
            return await Task.Run(() =>
            {
                using (var adapter = new SqlDataAdapter(_cmd))
                {
                    var ds = new DataSet();
                    adapter.Fill(ds);
                    adapter.Dispose();

                    return ds;
                }
            });
        }

        public DataTableRows GetRows()
        {
            using (SqlDataReader reader = _cmd.ExecuteReader())
            {
                DataTable dt = new DataTable();
                dt.Load(reader);
                reader.Close();
                
                return new DataTableRows(dt.Rows);
            }
        }

        public async Task<DataTableRows> GetRowsAsync()
        {
            using (SqlDataReader reader = await _cmd.ExecuteReaderAsync())
            {
                DataTable dt = new DataTable();
                dt.Load(reader);
                reader.Close();

                return new DataTableRows(dt.Rows);
            }
        }

        public Dictionary<string, Type> GetColumns()
        {
            using (SqlDataReader reader = _cmd.ExecuteReader())
            {
                var output = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < reader.FieldCount; ++i)
                {
                    output.Add(reader.GetName(i), reader.GetFieldType(i));
                }
                reader.Close();

                return output;
            }
        }

        public async Task<Dictionary<string, Type>> GetColumnsAsync()
        {
            using (SqlDataReader reader = await _cmd.ExecuteReaderAsync())
            {
                var output = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < reader.FieldCount; ++i)
                {
                    output.Add(reader.GetName(i), reader.GetFieldType(i));
                }
                reader.Close();

                return output;
            }
        }
    }
}
