using DiGi.Core.IO.Table.Interfaces;
using System;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
        public static string? DataTypeName(this IColumn? column)
        {
            return DataTypeName(column?.Type);
        }

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