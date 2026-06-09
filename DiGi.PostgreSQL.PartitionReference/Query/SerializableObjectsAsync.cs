using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionReference
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves a list of serializable objects associated with a specific partition name.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of the serializable object, which must implement ISerializableObject.</typeparam>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the database query.</param>
        /// <param name="name">The name of the partition from which to retrieve objects.</param>
        /// <returns>A task that represents the asynchronous operation, containing a list of serializable objects or null if the connection is null.</returns>
        public static async Task<List<USerializableObject>?> SerializableObjectsAsync<USerializableObject>(NpgsqlConnection? npgsqlConnection, string name) where USerializableObject : ISerializableObject
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            List<USerializableObject> result = [];

            short? partitionId = await PostgreSQL.Query.PartitionIdAsync(npgsqlConnection, name);
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

        /// <summary>
        /// Asynchronously retrieves a list of serializable objects associated with a collection of partition references.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of the serializable object, which must implement ISerializableObject.</typeparam>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the database query.</param>
        /// <param name="partitionReferences">A collection of partition references used to identify the objects to retrieve.</param>
        /// <returns>A task that represents the asynchronous operation, containing a list of serializable objects or null if the connection or partition references are null.</returns>
        public static async Task<List<USerializableObject>?> SerializableObjectsAsync<USerializableObject>(NpgsqlConnection? npgsqlConnection, IEnumerable<Classes.PartitionReference> partitionReferences) where USerializableObject : ISerializableObject
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

            List<USerializableObject> result = [];
            foreach (KeyValuePair<string, List<string>> keyValuePair in dictionary)
            {
                Partition? partition = await PostgreSQL.Query.PartitionAsync(npgsqlConnection, keyValuePair.Key);
                if (partition is null)
                {
                    continue;
                }

                string commandText = $@"
                SELECT o.data
                FROM objects_{(int)partition.DataType} o
                JOIN (
                    SELECT UNNEST(@partition_ids) as t_id, UNNEST(@unique_ids) as u_id
                ) as search_set ON o.partition_id = search_set.t_id AND o.unique_id = search_set.u_id;";

                await using var npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);

                npgsqlCommand.Parameters.Add("partition_ids", NpgsqlDbType.Array | NpgsqlDbType.Smallint);
                npgsqlCommand.Parameters.Add("unique_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);

                npgsqlCommand.Parameters["partition_ids"].Value = new short[] { partition.Id };
                npgsqlCommand.Parameters["unique_ids"].Value = keyValuePair.Value.ToArray();

                await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

                while (await npgsqlDataReader.ReadAsync())
                {
                    USerializableObject? serializableObject = await PostgreSQL.Query.SerializableObjectAsync<USerializableObject>(npgsqlDataReader, partition.DataType, 0);
                    if (serializableObject is null)
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