using CoreCore.DB.Plugin.Shared.Database;
using MySql.Data.MySqlClient;

namespace Core.DB.Plugin.MySQL
{
    public static class DB_Action
    {
        public static void ExecuteCommitAction(string connectionString, Action<CORE_DB_Connection> action)
        {
            CORE_DB_Connection? dbConnection = null;

            try
            {
                using var connection = new MySqlConnection(connectionString);

                dbConnection = new CORE_DB_Connection(connection);

                action(dbConnection);

                dbConnection.Commit();
            }
            catch (Exception)
            {
                dbConnection?.RollBack();
                throw;
            }
            finally
            {
                dbConnection?.Dispose();
            }
        }

        public static T ExecuteCommitAction<T>(string connectionString, Func<CORE_DB_Connection, T> action)
        {
            CORE_DB_Connection? dbConnection = null;

            try
            {
                using var connection = new MySqlConnection(connectionString);

                dbConnection = new CORE_DB_Connection(connection);

                var result = action(dbConnection);

                dbConnection.Commit();

                return result;
            }
            catch (Exception)
            {
                dbConnection?.RollBack();
                throw;
            }
            finally
            {
                dbConnection?.Dispose();
            }
        }
    }
}