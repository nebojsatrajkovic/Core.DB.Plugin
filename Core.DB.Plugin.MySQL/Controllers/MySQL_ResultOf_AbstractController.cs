using Core.Shared.Models;
using CoreCore.DB.Plugin.Shared.Database;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;

namespace Core.DB.Plugin.MySQL.Controllers
{
    public abstract class MySQL_ResultOf_AbstractController : ControllerBase
    {
        readonly ILogger logger;
        string _sessionToken = null!;
        CORE_DB_Connection _DB_Connection = null!;
        readonly string connectionString = null!;

        protected MySQL_ResultOf_AbstractController(ILogger logger, string connectionString)
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

        private ResultOf _ExecuteCommitAction(Func<ResultOf> action, bool authenticate)
        {
            try
            {
                using var connection = new MySqlConnection(connectionString);

                using var dbConnection = new CORE_DB_Connection(connection);

                _DB_Connection = dbConnection;
                _sessionToken = GetSessionToken();

                if (authenticate)
                {
                    Authenticate(dbConnection);
                }

                var result = action();

                if (result.Succeeded)
                {
                    _DB_Connection.Commit();
                }
                else
                {
                    _DB_Connection.RollBack();
                }

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

        private ResultOf<T> _ExecuteCommitAction<T>(Func<ResultOf<T>> action, bool authenticate)
        {
            using var connection = new MySqlConnection(connectionString);

            using var dbConnection = new CORE_DB_Connection(connection);

            _DB_Connection = dbConnection;
            _sessionToken = GetSessionToken();

            try
            {
                if (authenticate)
                {
                    Authenticate(_DB_Connection);
                }

                var result = action();

                if (result.Succeeded)
                {
                    _DB_Connection.Commit();
                }
                else
                {
                    _DB_Connection.RollBack();
                }

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

        #region resultof commit with auth

        protected ResultOf ExecuteCommitAction(Func<ResultOf> action)
        {
            return _ExecuteCommitAction(action, true);
        }

        protected ResultOf<T> ExecuteCommitAction<T>(Func<ResultOf<T>> action)
        {
            return _ExecuteCommitAction(action, true);
        }

        #endregion resultof commit with auth

        #region resultof commit with no auth

        protected ResultOf ExecuteUnauthenticatedCommitAction(Func<ResultOf> action)
        {
            return _ExecuteCommitAction(action, false);
        }

        protected ResultOf<T> ExecuteUnauthenticatedCommitAction<T>(Func<ResultOf<T>> action)
        {
            return _ExecuteCommitAction(action, false);
        }

        #endregion resultof commit with no auth

        protected abstract string GetSessionToken();

        protected abstract void Authenticate(CORE_DB_Connection dbConnection);
    }
}