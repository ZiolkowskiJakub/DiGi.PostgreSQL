using DiGi.Core.Interfaces;
using DiGi.Core.IO;
using DiGi.Core.IO.Classes;
using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<USerializableObject?> SerializableObject<USerializableObject>(NpgsqlDataReader npgsqlDataReader, Enums.DataType dataType, int index = 0) where USerializableObject : ISerializableObject
        {
            if (npgsqlDataReader is null || index == -1 || dataType == Enums.DataType.Undefined)
            {
                return default;
            }

            if (dataType == Enums.DataType.Json)
            {
                string data = npgsqlDataReader.GetString(0);

                if (Core.Convert.ToDiGi<USerializableObject>(data) is not List<USerializableObject> serializableObjects || serializableObjects.Count == 0)
                {
                    return default;
                }

                if (serializableObjects[0] is not USerializableObject serializableObject)
                {
                    return default;
                }

                return serializableObject;
            }
            else if (dataType == Enums.DataType.Binary)
            {
                byte[] bytes = await npgsqlDataReader.GetFieldValueAsync<byte[]>(index);
                if (bytes is null || bytes.Length == 0)
                {
                    return default;
                }

                List<USerializableObject>? serializableObjects = Core.Convert.ToDiGi<USerializableObject>(bytes);

                if (serializableObjects is null || serializableObjects.Count == 0 || serializableObjects[0] is not USerializableObject serializableObject)
                {
                    return default;
                }

                return serializableObject;
            }
            else if (dataType == Enums.DataType.Archive)
            {
                byte[] bytes = await npgsqlDataReader.GetFieldValueAsync<byte[]>(index);
                if (bytes is null || bytes.Length == 0)
                {
                    return default;
                }

                Archive<USerializableObject>? archieve = Core.IO.Create.Archive<USerializableObject>(bytes);
                if(archieve is null)
                {
                    return default;
                }

                return archieve.Deserialize<USerializableObject>();
            }

            return default;
        }
    }
}