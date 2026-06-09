using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves a list of serializable objects from the database based on the specified type and inheritance settings.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of serializable object to retrieve, which must implement <see cref="ISerializableObject"/>.</typeparam>
        /// <param name="npgsqlConnection">The Npgsql connection used to execute the database query.</param>
        /// <param name="inheritance">A value indicating whether to include inherited types in the retrieval process.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of retrieved serializable objects, or null if the connection is null.</returns>
        public static async Task<List<USerializableObject>?> SerializableObjectsAsync<USerializableObject>(NpgsqlConnection? npgsqlConnection, bool inheritance = true) where USerializableObject : ISerializableObject
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            List<Partition>? partitions = null;
            if (inheritance)
            {
                partitions = await PartitionsAsync(npgsqlConnection, typeof(USerializableObject));
            }
            else
            {
                Partition? partition = await PostgreSQL.Query.PartitionAsync(npgsqlConnection, Core.Query.FullTypeName(typeof(USerializableObject)));
                if (partition is not null)
                {
                    partitions = [partition];
                }
            }

            if (partitions is null)
            {
                return null;
            }

            List<USerializableObject> result = [];
            if (partitions.Count == 0)
            {
                return result;
            }

            Dictionary<Enums.DataType, List<Partition>>? dictionary = Core.Convert.ToSystem_Dictionary(partitions, x => x.DataType);
            if (dictionary is null || dictionary.Count == 0)
            {
                return result;
            }

            foreach (KeyValuePair<Enums.DataType, List<Partition>> keyValuePair in dictionary)
            {
                string commandText = $@"
                SELECT data
                FROM objects_{(int)keyValuePair.Key}
                WHERE partition_id = ANY(@partition_ids);";

                await using var npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);

                npgsqlCommand.Parameters.AddWithValue("partition_ids", keyValuePair.Value.ConvertAll(x => x.Id).ToArray());

                await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

                while (await npgsqlDataReader.ReadAsync())
                {
                    USerializableObject? serializableObject = await PostgreSQL.Query.SerializableObjectAsync<USerializableObject>(npgsqlDataReader, keyValuePair.Key, 0);
                    if (serializableObject is null)
                    {
                        continue;
                    }

                    result.Add(serializableObject);
                }
            }

            return result;
        }

        /// <summary>
        /// Asynchronously retrieves a list of serializable objects from the database using a collection of unique references.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of serializable object to retrieve, which must implement <see cref="ISerializableObject"/>.</typeparam>
        /// <typeparam name="TUniqueReference">The type of the unique reference used for lookup, which must implement <see cref="IUniqueReference"/>.</typeparam>
        /// <param name="npgsqlConnection">The Npgsql connection used to execute the database query.</param>
        /// <param name="uniqueReferences">A collection of unique references to be resolved into serializable objects.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of retrieved serializable objects, or null if the connection or unique references are null.</returns>
        public static async Task<List<USerializableObject>?> SerializableObjectsAsync<USerializableObject, TUniqueReference>(NpgsqlConnection? npgsqlConnection, IEnumerable<TUniqueReference> uniqueReferences) where USerializableObject : ISerializableObject where TUniqueReference : IUniqueReference
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

            List<USerializableObject> result = [];
            foreach (KeyValuePair<string, List<string>> keyValuePair in dictionary)
            {
                List<Partition>? partitions = await PartitionsAsync(npgsqlConnection, keyValuePair.Key);
                if (partitions is null || partitions.Count == 0)
                {
                    continue;
                }

                Dictionary<Enums.DataType, List<Partition>>? dictionary_DataType = Core.Convert.ToSystem_Dictionary(partitions, x => x.DataType);
                if (dictionary_DataType is null || dictionary_DataType.Count == 0)
                {
                    continue;
                }

                foreach (KeyValuePair<Enums.DataType, List<Partition>> keyValuePair_DataType in dictionary_DataType)
                {
                    string commandText = $@"
                        SELECT o.data
                        FROM objects_{(int)keyValuePair_DataType.Key} o
                        JOIN (
                            SELECT UNNEST(@partition_ids) as t_id, UNNEST(@unique_ids) as u_id
                        ) as search_set ON o.partition_id = search_set.t_id AND o.unique_id = search_set.u_id;";

                    await using var npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);

                    npgsqlCommand.Parameters.Add("partition_ids", NpgsqlDbType.Array | NpgsqlDbType.Smallint);
                    npgsqlCommand.Parameters.Add("unique_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);
                    npgsqlCommand.Parameters["partition_ids"].Value = keyValuePair_DataType.Value.ConvertAll(x => x.Id).ToArray();
                    npgsqlCommand.Parameters["unique_ids"].Value = keyValuePair.Value.ToArray();

                    await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

                    while (await npgsqlDataReader.ReadAsync())
                    {
                        USerializableObject? serializableObject = await PostgreSQL.Query.SerializableObjectAsync<USerializableObject>(npgsqlDataReader, keyValuePair_DataType.Key, 0);
                        if (serializableObject is null)
                        {
                            continue;
                        }

                        result.Add(serializableObject);
                    }
                }
            }

            return result;
        }
    }
}