using DiGi.Core.Interfaces;
using DiGi.Core.IO.Table.Interfaces;
using NpgsqlTypes;
using System;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
        public static NpgsqlDbType? NpgsqlDbType(this IColumn? column)
        {
            if (column?.Type is not Type type)
            {
                return null;
            }

            return NpgsqlDbType(type);
        }

        public static NpgsqlDbType? NpgsqlDbType(this Type? type)
        {
            if(type is null)
            {
                return null;
            }

            Type underlyingType = Nullable.GetUnderlyingType(type) ?? type;

            // Using switch expression for clean, explicit mapping
            return underlyingType switch
            {
                // Numeric types
                _ when underlyingType == typeof(short) => NpgsqlTypes.NpgsqlDbType.Smallint,
                _ when underlyingType == typeof(ushort) => NpgsqlTypes.NpgsqlDbType.Integer,
                _ when underlyingType == typeof(int) => NpgsqlTypes.NpgsqlDbType.Integer,
                _ when underlyingType == typeof(uint) => NpgsqlTypes.NpgsqlDbType.Bigint,
                _ when underlyingType == typeof(long) => NpgsqlTypes.NpgsqlDbType.Bigint,
                _ when underlyingType == typeof(ulong) => NpgsqlTypes.NpgsqlDbType.Numeric,
                _ when underlyingType == typeof(float) => NpgsqlTypes.NpgsqlDbType.Real,
                _ when underlyingType == typeof(double) => NpgsqlTypes.NpgsqlDbType.Double,
                _ when underlyingType == typeof(decimal) => NpgsqlTypes.NpgsqlDbType.Numeric,

                // Text types
                _ when underlyingType == typeof(string) => NpgsqlTypes.NpgsqlDbType.Text,
                _ when underlyingType == typeof(char) => NpgsqlTypes.NpgsqlDbType.Char,

                // Boolean
                _ when underlyingType == typeof(bool) => NpgsqlTypes.NpgsqlDbType.Boolean,

                // Date and Time
                _ when underlyingType == typeof(DateTime) => NpgsqlTypes.NpgsqlDbType.TimestampTz,
                _ when underlyingType == typeof(DateTimeOffset) => NpgsqlTypes.NpgsqlDbType.TimestampTz,
                _ when underlyingType == typeof(DateOnly) => NpgsqlTypes.NpgsqlDbType.Date,
                _ when underlyingType == typeof(TimeOnly) => NpgsqlTypes.NpgsqlDbType.Time,
                _ when underlyingType == typeof(TimeSpan) => NpgsqlTypes.NpgsqlDbType.Interval,

                // Specialized types
                _ when underlyingType == typeof(Guid) => NpgsqlTypes.NpgsqlDbType.Uuid,
                _ when underlyingType == typeof(byte[]) => NpgsqlTypes.NpgsqlDbType.Bytea,

                _ when underlyingType.IsEnum => NpgsqlTypes.NpgsqlDbType.Text,

                _ when typeof(ISerializableObject).IsAssignableFrom(underlyingType) => NpgsqlTypes.NpgsqlDbType.Jsonb,

                // Default fallback for complex objects
                _ => null
            };
        }
    }
}