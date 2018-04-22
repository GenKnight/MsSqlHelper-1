using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Threading;
using MSSqlHelper.Interfaces;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MsSqlHelper.Repositories
{
    public class BaseSqlRepository : ITransactionalRepository
    {
        private const int DeadlockRetryCount = 10;
        private const int DeadlockRetryDelayMs = 10;
        private const int TimeoutRetryCount = 2;

        private string _dbConnStr;
        private IsolationLevel? _isolationLevel;
        private SqlTransaction _transaction;

        public SqlTransaction GetCurrentTransactionObject => _transaction;


        public BaseSqlRepository(string dbConnStr, IsolationLevel? isolationLevel)
        {
            _dbConnStr = dbConnStr;

            _isolationLevel = isolationLevel;
        }

        public BaseSqlRepository(string dbConnStr)
        {
            _dbConnStr = dbConnStr;
        }

        public void Commit()
        {
            if (_transaction == null)
                throw new Exception("Transaction is not specified");

            _transaction.Commit();
            _transaction.Dispose();
            _transaction = null;
        }

        public void Rollback()
        {
            if (_transaction == null)
                throw new Exception("Transaction is not specified");

            _transaction.Rollback();
            _transaction.Dispose();
            _transaction = null;
        }

        private SqlCommand CreateCommand(string sql, SqlConnection sqlCon, CommandType ct, params SqlParameter[] parameters)
        {
            _transaction = _isolationLevel != null ? sqlCon.BeginTransaction(_isolationLevel.Value) : sqlCon.BeginTransaction();

            var cmd = new SqlCommand(sql, sqlCon, _transaction);
            cmd.Parameters.AddRange(parameters);
            cmd.CommandType = ct;
            return cmd;
        }

        protected void ExecuteNonQuery(string sql, CommandType ct, params SqlParameter[] parameters)
        {
            foreach (var sqlParameter in parameters.Where(p => p.Value == null && p.SqlDbType != SqlDbType.Structured))
            {
                sqlParameter.Value = DBNull.Value;
            }

            using (var sqlCon = new SqlConnection(_dbConnStr))
            {
                sqlCon.Open();

                var func = new Action(() =>
                {
                    using (var cmd = CreateCommand(sql, sqlCon, ct, parameters))
                    {
                        try
                        {
                            cmd.ExecuteNonQuery();
                            Commit();

                        }
                        finally
                        {
                            // Release the parameters to allow reuse in case of deadlock
                            cmd.Parameters.Clear();
                        }
                    }
                });

                for (int attemptNo = 0; attemptNo < DeadlockRetryCount + 1; ++attemptNo)
                {
                    try
                    {
                        func();
                        return;
                    }
                    catch (SqlException e)
                    {
                        if (e.Number == 1205 && attemptNo < DeadlockRetryCount) // Deadlocks
                        {
                            OnDeadlockDetected?.Invoke(this, null);
                            Thread.Sleep(DeadlockRetryDelayMs);
                        }
                        else if ((e.Number == -2 || e.Number == 11) && attemptNo < TimeoutRetryCount) // Timeouts
                        {
                            OnDeadlockDetected?.Invoke(this, null);
                            Thread.Sleep(DeadlockRetryDelayMs);
                        }
                        else
                        {
                            Rollback();
                            throw;
                        }
                    }
                }
            }
            // Should never reach this point
            throw new Exception("INTERNAL ERROR - SQL retry failsafe activated");
        }

        protected async Task ExecuteNonQueryAsync(string sql, CommandType ct, params SqlParameter[] parameters)
        {
            foreach (var sqlParameter in parameters.Where(p => p.Value == null && p.SqlDbType != SqlDbType.Structured))
            {
                sqlParameter.Value = DBNull.Value;
            }

            using (var sqlCon = new SqlConnection(_dbConnStr))
            {
                await sqlCon.OpenAsync();

                var executeTask = Task.Run(async () =>
                {
                    using (var cmd = CreateCommand(sql, sqlCon, ct, parameters))
                    {
                        try
                        {
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                            Commit();
                        }
                        finally
                        {
                            // Release the parameters to allow reuse in case of deadlock
                            cmd.Parameters.Clear();
                        }
                    }

                });

                for (int attemptNo = 0; attemptNo < DeadlockRetryCount + 1; ++attemptNo)
                {
                    try
                    {
                        await executeTask;
                        return;
                    }
                    catch (SqlException e)
                    {
                        if (e.Number == 1205 && attemptNo < DeadlockRetryCount) // Deadlocks
                        {
                            OnDeadlockDetected?.Invoke(this, null);
                            Thread.Sleep(DeadlockRetryDelayMs);
                        }
                        else if ((e.Number == -2 || e.Number == 11) && attemptNo < TimeoutRetryCount) // Timeouts
                        {
                            OnDeadlockDetected?.Invoke(this, null);
                            Thread.Sleep(DeadlockRetryDelayMs);
                        }
                        else
                        {
                            Rollback();
                            throw;
                        }
                    }
                }
            }

            // Should never reach this point
            throw new Exception("INTERNAL ERROR - SQL retry failsafe activated");

        }

        public event EventHandler OnDeadlockDetected;

        protected DataTableRows ExecuteProcedureOrSql(string sql, CommandType ct, params SqlParameter[] parameters)
        {
            foreach (var sqlParameter in parameters.Where(p => p.Value == null && p.SqlDbType != SqlDbType.Structured))
            {
                sqlParameter.Value = DBNull.Value;
            }

            using (var sqlCon = new SqlConnection(_dbConnStr))
            {
                sqlCon.Open();

                var func = new Func<DataTableRows>(() =>
                {
                    using (var cmd = CreateCommand(sql, sqlCon, ct, parameters))
                    {
                        try
                        {
                            using (var reader = new DataSetReader(cmd))
                            {
                                return reader.GetRows();
                            }
                        }
                        finally
                        {
                            Commit();
                            //Release the parameters to allow reuse in case of deadlock
                            cmd.Parameters.Clear();
                        }
                    }
                });

                var attemptNo = 0;
                for (int failSafe = 0; failSafe < DeadlockRetryCount + 1; ++failSafe)
                {
                    try
                    {
                        return func();
                    }
                    catch (SqlException e)
                    {
                        if (e.Number == 1205 && attemptNo++ < DeadlockRetryCount)
                        {
                            OnDeadlockDetected?.Invoke(this, null);
                            Thread.Sleep(DeadlockRetryDelayMs);
                        }
                        else
                        {
                            Rollback();
                            throw;
                        }
                    }
                }
            }
            // Should never reach this point
            throw new Exception("INTERNAL ERROR - SQL retry failsafe activated");
        }

        protected async Task<DataTableRows> ExecuteProcedureOrSqlAsync(string sql, CommandType ct, params SqlParameter[] parameters)
        {
            foreach (var sqlParameter in parameters.Where(p => p.Value == null && p.SqlDbType != SqlDbType.Structured))
            {
                sqlParameter.Value = DBNull.Value;
            }

            using (var sqlCon = new SqlConnection(_dbConnStr))
            {
                await sqlCon.OpenAsync();

                var executeTask = Task.Run(async () =>
                {
                    using (var cmd = CreateCommand(sql, sqlCon, ct, parameters))
                    {
                        try
                        {
                            using (var reader = new DataSetReader(cmd))
                            {
                                return await reader.GetRowsAsync().ConfigureAwait(false);
                            }
                        }
                        finally
                        {
                            Commit();
                            //Release the parameters to allow reuse in case of deadlock
                            cmd.Parameters.Clear();
                        }
                    }

                });

                var attemptNo = 0;
                for (int failSafe = 0; failSafe < DeadlockRetryCount + 1; ++failSafe)
                {
                    try
                    {
                        return await executeTask;
                    }
                    catch (SqlException e)
                    {
                        if (e.Number == 1205 && attemptNo++ < DeadlockRetryCount)
                        {
                            OnDeadlockDetected?.Invoke(this, null);
                            Thread.Sleep(DeadlockRetryDelayMs);
                        }
                        else
                        {
                            Rollback();
                            throw;
                        }
                    }
                }
            }
            // Should never reach this point
            throw new Exception("INTERNAL ERROR - SQL retry failsafe activated");
        }

        //protected void BulkCopy(DataTable table, string destinationTable, int timeoutSec = 600)
        //{
        //    var sqlBulkCopy = _transaction != null
        //        ? new SqlBulkCopy(_sqlCon, SqlBulkCopyOptions.Default, _transaction)
        //        : new SqlBulkCopy(_sqlCon);
        //    using (var bulkCopy = sqlBulkCopy)
        //    {
        //        // Add columm mapping
        //        for (int i = 0; i < table.Columns.Count; ++i)
        //            bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(table.Columns[i].ColumnName, table.Columns[i].ColumnName));

        //        bulkCopy.BulkCopyTimeout = timeoutSec;
        //        bulkCopy.DestinationTableName = destinationTable;
        //        bulkCopy.WriteToServer(table);
        //    }
        //}

        public DataTable FreeExecute(string sql)
        {
            using (var sqlCon = new SqlConnection(_dbConnStr))
            using (var cmd = CreateCommand(sql, sqlCon, CommandType.Text))
            using (var adapter = new SqlDataAdapter(cmd))
            {
                try
                {
                    sqlCon.Open();
                    var ds = new DataSet();
                    adapter.Fill(ds);
                    return ds.Tables[0];
                }
                finally
                {
                    cmd.Parameters.Clear();
                    sqlCon.Close();
                }
            }
        }

        public async Task<DataTable> FreeExecuteAsync(string sql)
        {
            return await Task.Run(async () =>
            {
                using (var sqlCon = new SqlConnection(_dbConnStr))
                using (var cmd = CreateCommand(sql, sqlCon, CommandType.Text))
                using (var adapter = new SqlDataAdapter(cmd))
                {
                    try
                    {
                        await sqlCon.OpenAsync();
                        var ds = new DataSet();
                        adapter.Fill(ds);
                        return ds.Tables[0];
                    }
                    finally
                    {
                        cmd.Parameters.Clear();
                        sqlCon.Close();
                    }
                }
            });
        }

        public void Dispose()
        {

        }
    }
}
