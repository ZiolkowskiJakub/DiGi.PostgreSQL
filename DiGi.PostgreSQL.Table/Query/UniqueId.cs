using DiGi.Core.IO.Table.Interfaces;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
        /// <summary>
        /// Generates a unique identifier for the specified column by normalizing its name.
        /// </summary>
        /// <param name="column">The column instance to process.</param>
        /// <returns>A normalized string representing the unique identifier, or null if the column or its name is null.</returns>
        public static string? UniqueId(this IColumn? column)
        {
            return Core.IO.Query.UniqueId(column);
        }
    }
}