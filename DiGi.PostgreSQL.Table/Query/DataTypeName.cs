using DiGi.Core.Interfaces;
using DiGi.Core.IO.Table.Interfaces;
using System;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
        public static string? DataTypeName(this IColumn? column)
        {
            if (column?.Type is not Type type)
            {
                return null;
            }

            Type underlyingType = Nullable.GetUnderlyingType(type) ?? type;

            // Using switch expression for clean, explicit mapping
            return underlyingType switch
            {
                // Numeric types
                _ when underlyingType == typeof(short) => "smallint",
                _ when underlyingType == typeof(int) => "integer",
                _ when underlyingType == typeof(long) => "bigint",
                _ when underlyingType == typeof(float) => "real",
                _ when underlyingType == typeof(double) => "double precision",
                _ when underlyingType == typeof(decimal) => "numeric",

                // Text types
                _ when underlyingType == typeof(string) => "text",
                _ when underlyingType == typeof(char) => "char(1)",

                // Boolean
                _ when underlyingType == typeof(bool) => "boolean",

                // Date and Time
                _ when underlyingType == typeof(DateTime) => "timestamptz",
                _ when underlyingType == typeof(DateTimeOffset) => "timestamptz",
                _ when underlyingType == typeof(DateOnly) => "date",
                _ when underlyingType == typeof(TimeOnly) => "time",
                _ when underlyingType == typeof(TimeSpan) => "interval",

                // Specialized types
                _ when underlyingType == typeof(Guid) => "uuid",
                _ when underlyingType == typeof(byte[]) => "bytea",

                _ when underlyingType.IsEnum => "text",

                _ when typeof(ISerializableObject).IsAssignableFrom(underlyingType) => "jsonb",

                // Default fallback for complex objects
                _ => null
            };
        }
    }
}