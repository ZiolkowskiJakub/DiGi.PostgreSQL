using DiGi.Core.Interfaces;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<List<USerializableObject>?> SerializableObjects<USerializableObject>(NpgsqlConnection? npgsqlConnection, bool inheritance = true) where USerializableObject : ISerializableObject
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            Dictionary<short, Type>? dictionary = null;
            if (inheritance)
            {
                dictionary = await Query.TypeIds(npgsqlConnection, typeof(USerializableObject));
            }
            else
            {
                Type type = typeof(USerializableObject);
                short? typeId = await Query.TypeId(npgsqlConnection, type);
                if (typeId.HasValue)
                {
                    dictionary = new Dictionary<short, Type>()
                    {
                        { typeId.Value, type }
                    };
                }
            }

            if (dictionary is null)
            {
                return null;
            }

            List<USerializableObject> result = [];
            if (dictionary.Count == 0)
            {
                return result;
            }

            string commandText = @"
                SELECT data
                FROM objects
                WHERE type_id = ANY(@type_ids);";

            await using var npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);

            npgsqlCommand.Parameters.AddWithValue("type_ids", dictionary.Keys.ToArray());

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            while (await npgsqlDataReader.ReadAsync())
            {
                string data = npgsqlDataReader.GetString(0);

                if (Core.Convert.ToDiGi<USerializableObject>(data) is not List<USerializableObject> serializableObjects || serializableObjects.Count == 0)
                {
                    continue;
                }

                if (serializableObjects[0] is not USerializableObject serializableObject)
                {
                    continue;
                }

                result.Add(serializableObject);
            }

            return result;
        }

        public static async Task<List<USerializableObject>?> SerializableObjects<USerializableObject, TUniqueReference>(NpgsqlConnection? npgsqlConnection, IEnumerable<TUniqueReference> uniqueReferences) where USerializableObject : ISerializableObject where TUniqueReference : IUniqueReference
        {
            if (npgsqlConnection is null || uniqueReferences is null)
            {
                return null;
            }

            Dictionary<string, List<string>> dictionary = [];
            foreach (TUniqueReference uniqueReference in uniqueReferences)
            {
                if (uniqueReference?.TypeReference?.FullTypeName is not string fullTypeName || uniqueReference.UniqueId is not string uniqueId)
                {
                    continue;
                }

                if (!dictionary.TryGetValue(fullTypeName, out List<string>? uniqueIds) || uniqueIds is null)
                {
                    uniqueIds = [];
                    dictionary[fullTypeName] = uniqueIds;
                }

                uniqueIds.Add(uniqueId);
            }

            string commandText = @"
                SELECT o.data
                FROM objects o
                JOIN (
                    SELECT UNNEST(@type_ids) as t_id, UNNEST(@unique_ids) as u_id
                ) as search_set ON o.type_id = search_set.t_id AND o.unique_id = search_set.u_id;";

            await using var npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);

            npgsqlCommand.Parameters.Add("type_ids", NpgsqlDbType.Array | NpgsqlDbType.Smallint);
            npgsqlCommand.Parameters.Add("unique_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);

            List<USerializableObject> result = [];
            foreach (KeyValuePair<string, List<string>> keyValuePair in dictionary)
            {
                Dictionary<short, Type>? dictionary_Types = await Query.TypeIds(npgsqlConnection, keyValuePair.Key);
                if (dictionary_Types is null || dictionary_Types.Count == 0)
                {
                    continue;
                }

                npgsqlCommand.Parameters["type_ids"].Value = dictionary_Types.Keys.ToArray();
                npgsqlCommand.Parameters["unique_ids"].Value = keyValuePair.Value.ToArray();

                await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

                while (await npgsqlDataReader.ReadAsync())
                {
                    string data = npgsqlDataReader.GetString(0);

                    if (Core.Convert.ToDiGi<USerializableObject>(data) is not List<USerializableObject> serializableObjects || serializableObjects.Count == 0)
                    {
                        continue;
                    }

                    if (serializableObjects[0] is not USerializableObject serializableObject)
                    {
                        continue;
                    }

                    result.Add(serializableObject);
                }
            }

            return result;
        }
    }
}