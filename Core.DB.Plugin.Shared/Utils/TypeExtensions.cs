using Core.DB.Plugin.Shared.Attributes;
using Core.DB.Plugin.Shared.Extensions;
using System.Reflection;

namespace Core.DB.Plugin.Shared.Utils
{
    public static class TypeExtensions
    {
        public static PropertyInfo? GetPrimaryKeyProperty(this Type t)
        {
            var property = t.GetProperties().FirstOrDefault(x => x.CustomAttributes.HasValue() && x.CustomAttributes.Any(ca => ca.AttributeType == typeof(CORE_DB_SQL_PrimaryKey)) == true);

            return property;
        }

        public static List<PropertyInfo> GetFilteredProperties(this Type t)
        {
            var properties = t.GetProperties().Where(x => !(x.CustomAttributes?.Any(a => a.AttributeType == typeof(CORE_DB_SQL_Ignore)) == true)).ToList();

            return properties;
        }

        public static List<PropertyInfo> GetFilteredProperties_ForAutoIncrementSave(this Type t)
        {
            var properties = t.GetProperties().Where(x => !(x.CustomAttributes?.Any(a => a.AttributeType == typeof(CORE_DB_SQL_PrimaryKey) || a.AttributeType == typeof(CORE_DB_SQL_Ignore)) == true)).ToList();

            return properties;
        }
    }
}