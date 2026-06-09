using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves a list of serializable objects based on the provided PostgreSQL connection and partition unique references.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of serializable object to retrieve, which must implement ISerializableObject.</typeparam>
        /// <param name="npgsqlConnection">The Npgsql connection used to execute the query.</param>
        /// <param name="partitionUniqueReferences">A collection of partition unique references to filter the objects.</param>
        /// <returns>A task that represents the asynchronous operation, containing a list of serializable objects if successful; otherwise, null.</returns>
        public static async Task<List<USerializableObject>?> SerializableObjectsAsync<USerializableObject>(NpgsqlConnection? npgsqlConnection, IEnumerable<Classes.PartitionUniqueReference> partitionUniqueReferences) where USerializableObject : ISerializableObject
        {
            if (npgsqlConnection is null || partitionUniqueReferences is null)
            {
                return null;
            }

            List<string> fullTypeNames = partitionUniqueReferences
                .Select(x => x.UniqueReference?.TypeReference?.FullTypeName)
                .Where(name => !string.IsNullOrEmpty(name))
                .Distinct()
                .Cast<string>()
                .ToList();

            List<string> names = partitionUniqueReferences
                .Select(x => x.Name)
                .Where(name => !string.IsNullOrEmpty(name))
                .Distinct()
                .Cast<string>()
                .ToList();

            if (fullTypeNames.Count == 0 || names.Count == 0)
            {
                return [];
            }

            Dictionary<string, Classes.Type> dictionary_Type = [];
            foreach (string fullTypeName in fullTypeNames)
            {
                Classes.Type? type = await TypeAsync(npgsqlConnection, fullTypeName);
                if (type is not null)
                {
                    dictionary_Type[fullTypeName] = type;
                }
            }

            Dictionary<string, Partition> dictionary_Partition = [];
            foreach (string name in names)
            {
                Partition? partition = await PostgreSQL.Query.PartitionAsync(npgsqlConnection, name);
                if (partition != null) dictionary_Partition[name] = partition;
            }

            List<USerializableObject> result = [];

            var groups = partitionUniqueReferences
                .Where(x => x.UniqueReference?.TypeReference?.FullTypeName != null && x.Name != null)
                .GroupBy(x => new
                {
                    TypeName = x.UniqueReference!.TypeReference!.FullTypeName!,
                    PartitionName = x.Name!
                });

            foreach (var group in groups)
            {
                if (group is null)
                {
                    continue;
                }

                if (!dictionary_Type.TryGetValue(group.Key.TypeName, out Classes.Type? type) || !dictionary_Partition.TryGetValue(group.Key.PartitionName, out Partition? partition))
                {
                    continue;
                }

                string?[] uniqueIds = [.. group.Select(x => x.UniqueReference!.UniqueId)];

                string commandText = $@"
                    SELECT data
                    FROM objects_{(int)partition.DataType}
                    WHERE partition_id = @partition_id
                      AND type_id = @type_id
                      AND unique_id = ANY(@unique_ids)";

                try
                {
                    await using NpgsqlCommand npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);
                    npgsqlCommand.Parameters.AddWithValue("partition_id", partition.Id);
                    npgsqlCommand.Parameters.AddWithValue("type_id", type.Id);
                    npgsqlCommand.Parameters.AddWithValue("unique_ids", uniqueIds);

                    await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

                    while (await npgsqlDataReader.ReadAsync())
                    {
                        USerializableObject? serializableObject = await PostgreSQL.Query.SerializableObjectAsync<USerializableObject>(npgsqlDataReader, partition.DataType, 0);

                        if (serializableObject != null)
                        {
                            result.Add(serializableObject);
                        }
                    }
                }
                catch (NpgsqlException ex)
                {
                    Console.WriteLine($"Postgres Error (SerializableObjectsAsync): {ex.Message}");
                    return null;
                }
            }

            return result;
        }
    }
}