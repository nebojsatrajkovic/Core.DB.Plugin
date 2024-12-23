using CoreCore.DB.Plugin.Shared.Database;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Core.DB.Plugin.PostgreSQL.Controllers
{
    public abstract class PostgreSQL_AbstractController : ControllerBase
    {
        readonly ILogger logger;
        string _sessionToken = null!;
        CORE_DB_Connection _DB_Connection = null!;
        readonly string connectionString = null!;

        protected PostgreSQL_AbstractController(ILogger logger, string connectionString)
        {
            this.logger = logger;
            this.connectionString = connectionString;
        }

        public CORE_DB_Connection DB_Connection
        {
            get
            {
                return _DB_Connection;
            }
            set
            {
                _DB_Connection = value;
            }
        }

        public string SessionToken
        {
            get
            {
                return _sessionToken;
            }
            set
            {
                _sessionToken = value;
            }
        }

        private void _ExecuteCommitAction(Action action, bool authenticate)
        {
            try
            {
                using var connection = new NpgsqlConnection(connectionString);

                using var dbConnection = new CORE_DB_Connection(connection);

                _DB_Connection = dbConnection;
                _sessionToken = GetSessionToken();

                if (authenticate)
                {
                    Authenticate();
                }

                action();

                _DB_Connection.Commit();

                _DB_Connection?.Dispose();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Failed to execute {action.Method.Name}");
                _DB_Connection?.RollBack();
                _DB_Connection?.Dispose();
                throw;
            }
        }

        private T _ExecuteCommitAction<T>(Func<T> action, bool authenticate)
        {
            using var connection = new NpgsqlConnection(connectionString);

            using var dbConnection = new CORE_DB_Connection(connection);

            _DB_Connection = dbConnection;
            _sessionToken = GetSessionToken();

            try
            {
                if (authenticate)
                {
                    Authenticate();
                }

                var result = action();

                _DB_Connection.Commit();

                _DB_Connection?.Dispose();

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Failed to execute {action.Method.Name}");
                _DB_Connection?.RollBack();
                _DB_Connection?.Dispose();
                throw;
            }
        }

        #region commit with auth

        protected void ExecuteCommitAction(Action action)
        {
            _ExecuteCommitAction(action, true);
        }

        protected T ExecuteCommitAction<T>(Func<T> action)
        {
            return _ExecuteCommitAction(action, true);
        }

        protected void ExecuteCommitActionTask(Action action)
        {
            Task.Factory.StartNew(() =>
            {
                _ExecuteCommitAction(action, true);
            });
        }

        protected Task<T> ExecuteCommitActionTask<T>(Func<T> action)
        {
            return Task.Factory.StartNew(() =>
            {
                return _ExecuteCommitAction(action, true);
            });
        }

        #endregion commit with auth

        #region commit with no auth

        protected void ExecuteUnauthenticatedCommitAction(Action action)
        {
            _ExecuteCommitAction(action, false);
        }

        protected T ExecuteUnauthenticatedCommitAction<T>(Func<T> action)
        {
            return _ExecuteCommitAction(action, false);
        }

        protected void ExecuteUnauthenticatedCommitActionTask(Action action)
        {
            Task.Factory.StartNew(() =>
            {
                _ExecuteCommitAction(action, false);
            });
        }

        protected Task<T> ExecuteUnauthenticatedCommitActionTask<T>(Func<T> action)
        {
            return Task.Factory.StartNew(() =>
            {
                return _ExecuteCommitAction(action, false);
            });
        }

        #endregion commit with no auth

        protected abstract string GetSessionToken();

        protected abstract void Authenticate();
    }
}