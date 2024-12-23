using System.ComponentModel;

namespace Core.DB.Plugin.Shared.Extensions
{
    public static class EnumExtensions
    {
        public static string Description(this Enum value)
        {
            return _GetEnumDescription(value);
        }

        public static int IntegerDescription(this Enum value)
        {
            return int.Parse(_GetEnumDescription(value));
        }

        private static string _GetEnumDescription(Enum value)
        {
            if (value.GetType().GetField(value.ToString())?.GetCustomAttributes(typeof(DescriptionAttribute), inherit: false) is DescriptionAttribute[] array && array.Length != 0)
            {
                return array[0].Description;
            }

            return value?.ToString() ?? string.Empty;
        }
    }
}