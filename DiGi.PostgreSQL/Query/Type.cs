using DiGi.PostgreSQL.Enums;
using System;
using System.Text.Json.Nodes;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Maps a PostgreSQL data type to its corresponding .NET system type.
        /// </summary>
        /// <param name="postgreSQLDataType">The PostgreSQL data type to convert.</param>
        /// <returns>The corresponding .NET <see cref="Type"/> if found; otherwise, <c>null</c>.</returns>
        public static Type? Type(this PostgreSQLDataType postgreSQLDataType)
        {
            if (postgreSQLDataType == PostgreSQLDataType.Undefined)
            {
                return null;
            }

            switch (postgreSQLDataType)
            {
                case PostgreSQLDataType.Integer:
                    return typeof(int);

                case PostgreSQLDataType.Bigint:
                    return typeof(long);

                case PostgreSQLDataType.Boolean:
                    return typeof(bool);

                case PostgreSQLDataType.CharacterVarying:
                    return typeof(string);

                case PostgreSQLDataType.TimestampWithoutTimeZone:
                    return typeof(DateTime);

                case PostgreSQLDataType.TimestampWithTimeZone:
                    return typeof(DateTime);

                case PostgreSQLDataType.Numeric:
                    return typeof(decimal);

                case PostgreSQLDataType.Uuid:
                    return typeof(Guid);

                case PostgreSQLDataType.Jsonb:
                    return typeof(JsonObject);

                case PostgreSQLDataType.Bytea:
                    return typeof(byte[]);

                case PostgreSQLDataType.Other:
                    return typeof(object);
            }

            return null;
        }
    }
}