using Core.Shared.Attributes;
using Core.Shared.Models;
using CoreCore.DB.Plugin.Shared.Database;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System.Reflection;

namespace Core.DB.Plugin.MySQL.Controllers
{
    public abstract class MySQL_ResultOf_AbstractController : ControllerBase
    {
        readonly ILogger logger;
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

        ResultOf _ExecuteCommitAction(Func<ResultOf> action, bool authenticate)
        {
            try
            {
                using var connection = new MySqlConnection(connectionString);

                using var dbConnection = new CORE_DB_Connection(connection);

                _DB_Connection = dbConnection;

                if (authenticate)
                {
                    Authenticate(dbConnection);

                    var requiredRights = action.Method.GetCustomAttributes<CORE_AUTH_RequiredRight>().Where(x => !string.IsNullOrEmpty(x.RequiredRight)).Select(x => x.RequiredRight).ToList();

                    if (requiredRights != null && requiredRights.Count > 0)
                    {
                        Authorize(_DB_Connection, [.. requiredRights]);
                    }
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

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to execute {method}", action.Method.Name);
                _DB_Connection?.RollBack();
                throw;
            }
        }

        ResultOf<T> _ExecuteCommitAction<T>(Func<ResultOf<T>> action, bool authenticate)
        {
            using var connection = new MySqlConnection(connectionString);

            using var dbConnection = new CORE_DB_Connection(connection);

            _DB_Connection = dbConnection;

            try
            {
                if (authenticate)
                {
                    Authenticate(_DB_Connection);

                    var requiredRights = action.Method.GetCustomAttributes<CORE_AUTH_RequiredRight>().Where(x => !string.IsNullOrEmpty(x.RequiredRight)).Select(x => x.RequiredRight).ToList();

                    if (requiredRights != null && requiredRights.Count > 0)
                    {
                        Authorize(_DB_Connection, [.. requiredRights]);
                    }
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

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to execute {method}", action.Method.Name);
                _DB_Connection?.RollBack();
                throw;
            }
        }

        async Task<ResultOf> _ExecuteCommitAction_Async(Func<Task<ResultOf>> action, bool authenticate)
        {
            try
            {
                using var connection = new MySqlConnection(connectionString);

                using var dbConnection = new CORE_DB_Connection(connection);

                _DB_Connection = dbConnection;

                if (authenticate)
                {
                    await AuthenticateAsync(dbConnection);

                    var requiredRights = action.Method.GetCustomAttributes<CORE_AUTH_RequiredRight>().Where(x => !string.IsNullOrEmpty(x.RequiredRight)).Select(x => x.RequiredRight).ToList();

                    if (requiredRights != null && requiredRights.Count > 0)
                    {
                        await AuthorizeAsync(_DB_Connection, [.. requiredRights]);
                    }
                }

                var result = await action();

                if (result.Succeeded)
                {
                    _DB_Connection.Commit();
                }
                else
                {
                    _DB_Connection.RollBack();
                }

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to execute {method}", action.Method.Name);
                _DB_Connection?.RollBack();
                throw;
            }
        }

        async Task<ResultOf<T>> _ExecuteCommitAction_Async<T>(Func<Task<ResultOf<T>>> action, bool authenticate)
        {
            using var connection = new MySqlConnection(connectionString);

            using var dbConnection = new CORE_DB_Connection(connection);

            _DB_Connection = dbConnection;

            try
            {
                if (authenticate)
                {
                    await AuthenticateAsync(_DB_Connection);

                    var requiredRights = action.Method.GetCustomAttributes<CORE_AUTH_RequiredRight>().Where(x => !string.IsNullOrEmpty(x.RequiredRight)).Select(x => x.RequiredRight).ToList();

                    if (requiredRights != null && requiredRights.Count > 0)
                    {
                        await AuthorizeAsync(_DB_Connection, [.. requiredRights]);
                    }
                }

                var result = await action();

                if (result.Succeeded)
                {
                    _DB_Connection.Commit();
                }
                else
                {
                    _DB_Connection.RollBack();
                }

                return result;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to execute {method}", action.Method.Name);
                _DB_Connection?.RollBack();
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

        protected async Task<ResultOf> ExecuteCommitAction_Async(Func<Task<ResultOf>> action)
        {
            return await _ExecuteCommitAction_Async(action, true);
        }

        protected async Task<ResultOf<T>> ExecuteCommitAction_Async<T>(Func<Task<ResultOf<T>>> action)
        {
            return await _ExecuteCommitAction_Async(action, true);
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

        protected async Task<ResultOf> ExecuteUnauthenticatedCommitAction_Async(Func<Task<ResultOf>> action)
        {
            return await _ExecuteCommitAction_Async(action, false);
        }

        protected async Task<ResultOf<T>> ExecuteUnauthenticatedCommitAction_Async<T>(Func<Task<ResultOf<T>>> action)
        {
            return await _ExecuteCommitAction_Async(action, false);
        }

        #endregion resultof commit with no auth

        protected abstract void Authenticate(CORE_DB_Connection dbConnection);
        protected abstract Task AuthenticateAsync(CORE_DB_Connection dbConnection);

        protected abstract void Authorize(CORE_DB_Connection connection, List<string> requiredRights);
        protected abstract Task AuthorizeAsync(CORE_DB_Connection connection, List<string> requiredRights);
    }
}