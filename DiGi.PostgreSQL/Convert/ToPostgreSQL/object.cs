using DiGi.Core;
using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Enums;
using NpgsqlTypes;

namespace DiGi.PostgreSQL
{
    public static partial class Convert
    {
        /// <summary>
        /// Converts a serializable object to a PostgreSQL-compatible format based on the specified data type.
        /// </summary>
        /// <param name="serializableObject">The object that implements <see cref="ISerializableObject"/> to be converted.</param>
        /// <param name="dataType">The target <see cref="DataType"/> for the conversion.</param>
        /// <param name="npgsqlDbType">When this method returns, contains the corresponding <see cref="NpgsqlDbType"/> for the PostgreSQL database.</param>
        /// <returns>An object representing the converted value in a format compatible with PostgreSQL, or null if no conversion is defined for the given data type.</returns>
        public static object? ToPostgreSQL(this ISerializableObject serializableObject, DataType dataType, out NpgsqlDbType npgsqlDbType)
        {
            npgsqlDbType = NpgsqlDbType.Unknown;

            switch (dataType)
            {
                case DataType.Json:
                    npgsqlDbType = NpgsqlDbType.Jsonb;
                    return serializableObject.ToSystem_String();

                case DataType.Binary:
                    npgsqlDbType = NpgsqlDbType.Bytea;
                    return serializableObject.ToSystem_Bytes();

                case DataType.Archive:
                    npgsqlDbType = NpgsqlDbType.Bytea;
                    return Core.IO.Query.Serialize(serializableObject)?.Bytes;
            }

            return null;
        }
    }
}