using DiGi.Core;
using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Enums;
using NpgsqlTypes;

namespace DiGi.PostgreSQL
{
    public static partial class Convert
    {
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