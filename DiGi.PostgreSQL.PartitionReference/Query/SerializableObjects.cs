using DiGi.Core.Interfaces;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionReference
{
    public static partial class Query
    {
        public static async Task<List<USerializableObject>?> SerializableObjects<USerializableObject>(NpgsqlConnection? npgsqlConnection, string name) where USerializableObject : ISerializableObject
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            List<USerializableObject> result = [];

            short? partitionId = await PostgreSQL.Query.PartitionId(npgsqlConnection, name);
            if (partitionId is null)
            {
                return result;
            }

            string commandText = @"
                SELECT data
                FROM objects
                WHERE partition_id = ANY(@partition_ids);";

            await using var npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);

            npgsqlCommand.Parameters.AddWithValue("partition_ids", new short[] { partitionId.Value });

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

        public static async Task<List<USerializableObject>?> SerializableObjects<USerializableObject>(NpgsqlConnection? npgsqlConnection, IEnumerable<Classes.PartitionReference> partitionReferences) where USerializableObject : ISerializableObject
        {
            if (npgsqlConnection is null || partitionReferences is null)
            {
                return null;
            }

            Dictionary<string, List<string>> dictionary = [];
            foreach (Classes.PartitionReference partitionReference in partitionReferences)
            {
                if (partitionReference?.Name is not string name || partitionReference.UniqueId is not string uniqueId)
                {
                    continue;
                }

                if (!dictionary.TryGetValue(name, out List<string>? uniqueIds) || uniqueIds is null)
                {
                    uniqueIds = [];
                    dictionary[name] = uniqueIds;
                }

                uniqueIds.Add(uniqueId);
            }

            string commandText = @"
                SELECT o.data
                FROM objects o
                JOIN (
                    SELECT UNNEST(@partition_ids) as t_id, UNNEST(@unique_ids) as u_id
                ) as search_set ON o.partition_id = search_set.t_id AND o.unique_id = search_set.u_id;";

            await using var npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);

            npgsqlCommand.Parameters.Add("partition_ids", NpgsqlDbType.Array | NpgsqlDbType.Smallint);
            npgsqlCommand.Parameters.Add("unique_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);

            List<USerializableObject> result = [];
            foreach (KeyValuePair<string, List<string>> keyValuePair in dictionary)
            {
                short? partitionId = await PostgreSQL.Query.PartitionId(npgsqlConnection, keyValuePair.Key);
                if (partitionId is null)
                {
                    continue;
                }

                npgsqlCommand.Parameters["partition_ids"].Value = new short[] { partitionId.Value };
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