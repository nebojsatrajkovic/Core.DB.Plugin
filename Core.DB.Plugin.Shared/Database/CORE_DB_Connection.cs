using System.Data;
using System.Data.Common;

namespace CoreCore.DB.Plugin.Shared.Database
{
    public class CORE_DB_Connection : IDisposable
    {
        readonly DbConnection _Connection = null!;
        DbTransaction _Transaction = null!;

        public DbConnection Connection => _Connection;
        public DbTransaction Transaction => _Transaction;

        public CORE_DB_Connection(DbConnection connection)
        {
            _Connection = connection;

            connection.Open();

            _Transaction = connection.BeginTransaction();
        }

        public void Dispose()
        {
            CloseConnection();
            _Transaction.Dispose();

            GC.SuppressFinalize(this);
        }

        public void Commit()
        {
            if (_Connection.State == ConnectionState.Open && _Transaction != null)
            {
                _Transaction.Commit();
            }
        }

        public void CommitAndBeginNewTransaction()
        {
            if (_Connection.State == ConnectionState.Open && _Transaction != null)
            {
                _Transaction.Commit();
                _Transaction = _Connection.BeginTransaction();
            }
        }

        public void BeginNewTransaction()
        {
            if (_Connection.State == ConnectionState.Open && _Transaction != null)
            {
                _Transaction = _Connection.BeginTransaction();
            }
        }

        public void RollBack()
        {
            if (_Connection.State == ConnectionState.Open && _Transaction != null)
            {
                _Transaction.Rollback();
            }
        }

        public void RollBackAndBeginNewTransaction()
        {
            if (_Connection.State == ConnectionState.Open && _Transaction != null)
            {
                _Transaction.Rollback();
                _Transaction = _Connection.BeginTransaction();
            }
        }

        public void CloseConnection()
        {
            if (_Connection.State == ConnectionState.Open)
            {
                _Connection.Close();
            }
        }
    }
}