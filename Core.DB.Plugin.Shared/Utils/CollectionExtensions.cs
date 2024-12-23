namespace Core.DB.Plugin.Shared.Extensions
{
    public static class CollectionExtensions
    {
        public static bool IsEmpty<T>(this IEnumerable<T> collection)
        {
            return collection == null || !collection.Any();
        }

        public static bool HasValue<T>(this IEnumerable<T> collection)
        {
            return !IsEmpty(collection);
        }
    }
}