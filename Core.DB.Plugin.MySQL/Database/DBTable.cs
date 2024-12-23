using Core.DB.Plugin.Shared.Extensions;
using Core.DB.Plugin.Shared.Utils;
using CoreCore.DB.Plugin.Shared.Database;
using MySql.Data.MySqlClient;
using System.Reflection;
using System.Text;

namespace Core.DB.Plugin.MySQL.Database
{
    /// <summary>
    /// Contains all the logic for database table ORM manipulation
    /// </summary>
    /// <typeparam name="T1">Represents main database table model</typeparam>
    /// <typeparam name="T2">Represents model for data querying</typeparam>
    public class DBTable<T1, T2> where T1 : new() where T2 : new()
    {
        readonly string TableName = typeof(T1).DeclaringType?.Name ?? typeof(T1).Name;
        readonly PropertyInfo? PrimaryKeyProperty = typeof(T1).GetPrimaryKeyProperty();
        readonly List<PropertyInfo> Model_FilteredProperties = typeof(T1).GetFilteredProperties();
        readonly List<PropertyInfo> Query_FilteredProperties = typeof(T2).GetFilteredProperties();

        /// <summary>
        /// Search all table entries that match values passed by parameter
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public List<T1> Search(CORE_DB_Connection dbConnection, T2 parameter)
        {
            var result = new List<T1>();

            var queryString = $"SELECT * FROM {TableName} {GetWhereCondition(parameter)}";

            using var command = new MySqlCommand(queryString, (MySqlConnection)dbConnection.Connection, (MySqlTransaction)dbConnection.Transaction);

            using var reader = command.ExecuteReader();

            try
            {
                while (reader.Read())
                {
                    var item = new T1();

                    foreach (var property in Model_FilteredProperties)
                    {
                        if (property.PropertyType == typeof(bool))
                        {
                            var value = reader[property.Name];

                            if (value is sbyte val)
                            {
                                var boolValue = val > 0;

                                property.SetValue(item, boolValue);
                            }
                            else
                            {
                                // not supported currently

                                property.SetValue(item, false);
                            }
                        }
                        else
                        {
                            property.SetValue(item, reader[property.Name]);
                        }
                    }

                    result.Add(item);
                }
            }
            finally
            {
                reader.Close();
            }

            return result;
        }

        #region soft delete

        /// <summary>
        /// Soft delete single entry
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public int SoftDelete(CORE_DB_Connection dbConnection, T1 parameter)
        {
            return SoftDelete(dbConnection, new List<T1> { parameter });
        }

        /// <summary>
        /// Soft delete multiple entries
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public int SoftDelete(CORE_DB_Connection dbConnection, List<T1> parameter)
        {
            int result = 0;

            if (parameter.HasValue())
            {
                if (PrimaryKeyProperty != null)
                {
                    var ids = parameter.Select(x => PrimaryKeyProperty.GetValue(x, null)).ToList();

                    if (ids.HasValue() && DBTable<T1, T2>.GetParameterValues(ids, out var parameterValues))
                    {
                        var queryString = $"UPDATE {TableName} SET IsDeleted = 1 WHERE {PrimaryKeyProperty.Name} IN ({parameterValues})";

                        using var command = new MySqlCommand(queryString, (MySqlConnection)dbConnection.Connection, (MySqlTransaction)dbConnection.Transaction);

                        result = command.ExecuteNonQuery();
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Soft delete all entries that match values passed by parameter
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="parameter"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public int SoftDelete(CORE_DB_Connection dbConnection, T2 parameter)
        {
            var queryString = $"UPDATE {TableName} SET IsDeleted = 1 {GetWhereCondition(parameter)}";

            using var command = new MySqlCommand(queryString, (MySqlConnection)dbConnection.Connection, (MySqlTransaction)dbConnection.Transaction);

            var result = command.ExecuteNonQuery();

            return result;
        }

        #endregion soft delete

        #region hard delete

        /// <summary>
        /// Hard delete single entry
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public int Delete(CORE_DB_Connection dbConnection, T1 parameter)
        {
            return Delete(dbConnection, new List<T1> { parameter });
        }

        /// <summary>
        /// Hard delete multiple entries
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="parameter"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public int Delete(CORE_DB_Connection dbConnection, List<T1> parameter)
        {
            int result = 0;

            if (parameter.HasValue())
            {
                if (PrimaryKeyProperty != null)
                {
                    var ids = parameter.Select(x => PrimaryKeyProperty.GetValue(x, null)).ToList();

                    if (ids.HasValue() && DBTable<T1, T2>.GetParameterValues(ids, out var parameterValues))
                    {
                        var queryString = $"DELETE FROM {TableName} WHERE {PrimaryKeyProperty.Name} IN ({parameterValues})";

                        using var command = new MySqlCommand(queryString, (MySqlConnection)dbConnection.Connection, (MySqlTransaction)dbConnection.Transaction);

                        result = command.ExecuteNonQuery();
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Hard delete all entries that match values passed by parameter
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public int Delete(CORE_DB_Connection dbConnection, T2 parameter)
        {
            var queryString = $"DELETE FROM {TableName} {GetWhereCondition(parameter)}";

            using var command = new MySqlCommand(queryString, (MySqlConnection)dbConnection.Connection, (MySqlTransaction)dbConnection.Transaction);

            var result = command.ExecuteNonQuery();

            return result;
        }

        #endregion hard delete

        /// <summary>
        /// Saves the entry in the database.
        /// </summary>
        /// <param name="dbConnection"></param>
        /// <param name="parameter"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public T1 Save(CORE_DB_Connection dbConnection, T1 parameter)
        {
            var primaryKeyProperty = PrimaryKeyProperty ?? throw new Exception($"Primary key property not found for type {typeof(T1).Name}");

            var (columns, values) = GetColumnsAndValues(parameter);
            var onDuplicateKeyStatement = OnDuplicateKeyStatement();

            var queryString = $"INSERT INTO {TableName} ({columns}) VALUES ({values}) as statement ON DUPLICATE KEY UPDATE {onDuplicateKeyStatement}";

            using var command = new MySqlCommand(queryString, (MySqlConnection)dbConnection.Connection, (MySqlTransaction)dbConnection.Transaction);

            var result = command.ExecuteNonQuery();

            var id = Convert.ChangeType(command.LastInsertedId, primaryKeyProperty.PropertyType);

            primaryKeyProperty.SetValue(parameter, id);

            return parameter;
        }

        internal (string columns, string values) GetColumnsAndValues(T1 parameter)
        {
            var columnsBuilder = new StringBuilder();
            var valuesBuilder = new StringBuilder();

            foreach (var property in Model_FilteredProperties)
            {
                columnsBuilder.Append($"{property.Name}, ");

                var value = property.GetValue(parameter, null);

                if (value == null)
                {
                    valuesBuilder.Append("NULL, ");
                    continue;
                }

                var valueType = value.GetType();

                string segment;

                if
                (
                    valueType == typeof(double) ||
                    valueType == typeof(float) ||
                    valueType == typeof(decimal) ||
                    valueType == typeof(int)
                )
                {
                    segment = $"{value}";
                }
                else if (valueType == typeof(DateTime))
                {
                    segment = $"'{(DateTime)value:yyyy-MM-dd HH:mm:ss}'";
                }
                else if (valueType == typeof(bool))
                {
                    segment = (bool)value ? "1" : "0";
                }
                else
                {
                    segment = $"'{value}'";
                }

                segment = $"{segment}, ";

                valuesBuilder.Append(segment);
            }

            var columns = columnsBuilder.ToString();
            columns = columns[..^2];

            var values = valuesBuilder.ToString();
            values = values[..^2];

            return (columns, values);
        }

        internal string OnDuplicateKeyStatement()
        {
            var sb = new StringBuilder();

            foreach (var property in Model_FilteredProperties)
            {
                sb.Append($"{property.Name} = statement.{property.Name}, ");
            }

            var statement = sb.ToString();
            statement = statement[..^2];

            return statement;
        }

        internal string GetWhereCondition(T2 parameter)
        {
            if (parameter == null) { return string.Empty; }

            var builder = new StringBuilder();

            foreach (var property in Query_FilteredProperties)
            {
                if (property.GetValue(parameter, null) != null)
                {
                    if (builder.Length == 0)
                    {
                        builder.Append("WHERE ");
                    }

                    string segment;

                    if (property.PropertyType == typeof(DateTime?))
                    {
                        var dateParameter = ((DateTime?)property.GetValue(parameter, null))?.ToString("yyyy-MM-dd HH:mm:ss") ?? "0";

                        segment = $"{property.Name} = '{dateParameter}'";
                    }
                    else if (property.PropertyType == typeof(bool?))
                    {
                        if ((bool?)property.GetValue(parameter, null) == true)
                        {
                            segment = $"{property.Name} = 1";
                        }
                        else
                        {
                            segment = $"{property.Name} = 0";
                        }
                    }
                    else if
                        (
                        property.PropertyType == typeof(double?) ||
                        property.PropertyType == typeof(float?) ||
                        property.PropertyType == typeof(decimal?) ||
                        property.PropertyType == typeof(int?)
                        )
                    {
                        segment = $"{property.Name} = {property.GetValue(parameter, null)}";
                    }
                    else
                    {
                        segment = $"{property.Name} = '{property.GetValue(parameter, null)}'";
                    }

                    segment = $"{segment} AND ";

                    builder.Append(segment);
                }
            }

            var where = string.Empty;

            if (builder.Length > 0)
            {
                where = builder.ToString();
                where = where[..^5];
            }

            return where;
        }

        internal static bool GetParameterValues(List<object?> values, out string? parameterValues)
        {
            var builder = new StringBuilder();

            if (values.HasValue())
            {
                foreach (var value in values)
                {
                    if (value == null) { continue; }

                    var valueType = value.GetType();

                    string segment;

                    if
                    (
                        valueType == typeof(double) ||
                        valueType == typeof(float) ||
                        valueType == typeof(decimal) ||
                        valueType == typeof(int)
                    )
                    {
                        segment = $"{value}";
                    }
                    else
                    {
                        segment = $"'{value}'";
                    }

                    segment = $"{segment}, ";

                    builder.Append(segment);
                }
            }

            if (builder.Length > 0)
            {
                parameterValues = builder.ToString();
                parameterValues = parameterValues[..^2];

                return true;
            }
            else
            {
                parameterValues = null;
            }

            return false;
        }
    }
}