using DiGi.Core.IO.Table.Interfaces;
using System;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
        /// <summary>
        /// Gets the PostgreSQL data type name for the specified column.
        /// </summary>
        /// <param name="column">The column for which to get the data type name.</param>
        /// <returns>The PostgreSQL data type name as a string, or null if not found.</returns>
        public static string? DataTypeName(this IColumn? column)
        {
            return DataTypeName(column?.Type);
        }

        /// <summary>
        /// Gets the PostgreSQL data type name for the specified .NET type.
        /// </summary>
        /// <param name="type">The .NET type for which to get the data type name.</param>
        /// <returns>The PostgreSQL data type name as a string, or null if not found.</returns>
        public static string? DataTypeName(this Type? type)
        {
            NpgsqlTypes.NpgsqlDbType? npgsqlDbType = NpgsqlDbType(type);
            if (npgsqlDbType is null)
            {
                return null;
            }

            switch (npgsqlDbType)
            {
                case NpgsqlTypes.NpgsqlDbType.Double:
                    return "double precision";
            }

            return npgsqlDbType.ToString()?.ToLower();
        }
    }
}