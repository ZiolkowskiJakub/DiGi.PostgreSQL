using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.Enums;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Query
    {
        public static async Task<bool> ContainsAsync(this NpgsqlConnection npgsqlConnection, Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return false;
            }

            Classes.Type? type_Temp = await TypeAsync(npgsqlConnection, Core.Query.FullTypeName(type));
            if (type_Temp is null)
            {
                return false;
            }

            return await ContainsAsync(npgsqlConnection, type_Temp);
        }

        public static async Task<bool> ContainsAsync(this NpgsqlConnection npgsqlConnection, Classes.Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return false;
            }

            bool result = false;

            foreach (DataType dataType in Enum.GetValues<DataType>())
            {
                if (dataType == DataType.Undefined)
                {
                    continue;
                }

                string name = $@"objects_{(int)dataType}";

                bool tableExists = PostgreSQL.Query.TableExists(npgsqlConnection, name);
                if (!tableExists)
                {
                    continue;
                }

                await using NpgsqlCommand npgsqlCommand = new($"SELECT EXISTS(SELECT 1 FROM {name} WHERE type_id = @type_id LIMIT 1)", npgsqlConnection);
                npgsqlCommand.Parameters.AddWithValue("type_id", type.Id);

                var @var = await npgsqlCommand.ExecuteScalarAsync();

                result = @var is bool exists && exists;
                if (result)
                {
                    break;
                }
            }

            return result;
        }

        public static async Task<HashSet<Classes.PartitionUniqueReference>?> ContainsAsync(this NpgsqlConnection npgsqlConnection, IEnumerable<Classes.PartitionUniqueReference> partitionUniqueReferences)
        {
            if (npgsqlConnection is null || partitionUniqueReferences is null)
            {
                return null;
            }

            Dictionary<string, HashSet<Classes.PartitionUniqueReference>> dictionary = [];
            foreach (Classes.PartitionUniqueReference partitionUniqueReference in partitionUniqueReferences)
            {
                string? partitionName = partitionUniqueReference.Name;
                if (string.IsNullOrWhiteSpace(partitionName))
                {
                    continue;
                }

                if (partitionUniqueReference.UniqueReference?.UniqueId is not string uniqueId)
                {
                    continue;
                }

                if (dictionary.TryGetValue(partitionName, out HashSet<Classes.PartitionUniqueReference>? partitionUniqueReferences_Partition) || partitionUniqueReferences_Partition is null)
                {
                    partitionUniqueReferences_Partition = [];
                    dictionary[partitionName] = partitionUniqueReferences_Partition;
                }

                partitionUniqueReferences_Partition.Add(partitionUniqueReference);
            }

            HashSet<Classes.PartitionUniqueReference> result = [];

            foreach (KeyValuePair<string, HashSet<Classes.PartitionUniqueReference>> keyValuePair in dictionary)
            {
                Partition? partition = await PostgreSQL.Query.PartitionAsync(npgsqlConnection, keyValuePair.Key);
                if (partition is null)
                {
                    continue;
                }

                HashSet<IUniqueReference>? uniqueReferences = await ContainsAsync(npgsqlConnection, partition, keyValuePair.Value?.ToList().ConvertAll(x => x.UniqueReference!));
                if (uniqueReferences is null)
                {
                    continue;
                }

                List<Classes.PartitionUniqueReference>? partitionUniqueReferences_Result = keyValuePair.Value?.ToList().FindAll(x => uniqueReferences.Contains(x.UniqueReference!));
                if (partitionUniqueReferences_Result is null)
                {
                    continue;
                }

                partitionUniqueReferences_Result.ForEach(x => result.Add(x));
            }

            return result;
        }

        public static async Task<HashSet<IUniqueReference>?> ContainsAsync(this NpgsqlConnection npgsqlConnection, Partition? partition, IEnumerable<IUniqueReference>? uniqueReferences)
        {
            if (npgsqlConnection is null || partition is null || uniqueReferences is null)
            {
                return null;
            }

            // 1. Group references by Type Name to resolve IDs efficiently
            // Using a dictionary for O(1) lookups during result mapping
            Dictionary<string, List<IUniqueReference>> dictionary = uniqueReferences
                .Where(x => x?.TypeReference?.FullTypeName != null)
                .GroupBy(x => x.TypeReference!.FullTypeName!)
                .ToDictionary(g => g.Key, g => g.ToList());

            if (dictionary.Count == 0)
            {
                return [];
            }

            // 2. Resolve all Type IDs at once (Ideally these should be cached in a ConcurrentDictionary)
            List<short> typeIds = [];
            List<string> uniqueIds = [];

            // Map to store (type_id, unique_id) -> IUniqueReference for quick reconstruction
            Dictionary<(short, string), IUniqueReference> referenceLookup = [];

            foreach (KeyValuePair<string, List<IUniqueReference>> keyValuePair in dictionary)
            {
                // In a real API, replace this with a cached lookup to avoid DB roundtrips for types
                Classes.Type? type = await TypeAsync(npgsqlConnection, keyValuePair.Key);
                if (type == null)
                {
                    continue;
                }

                foreach (IUniqueReference uniqueReference in keyValuePair.Value)
                {
                    if (uniqueReference?.UniqueId is not string uniqueId)
                    {
                        continue;
                    }

                    typeIds.Add(type.Id);
                    uniqueIds.Add(uniqueId);
                    referenceLookup[(type.Id, uniqueId)] = uniqueReference;
                }
            }

            if (typeIds.Count == 0)
            {
                return [];
            }

            // 3. Single Batch Query using UNNEST
            // This joins our input arrays as a virtual table and matches against the objects table
            string commandText = $@"
                SELECT o.type_id, o.unique_id
                FROM unnest(@type_ids, @unique_ids) AS input(t_id, u_id)
                INNER JOIN objects_{(int)partition.DataType} o
                    ON o.partition_id = @partition_id
                    AND o.type_id = input.t_id
                    AND o.unique_id = input.u_id";

            HashSet<IUniqueReference> result = [];

            try
            {
                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                npgsqlCommand.Parameters.AddWithValue("partition_id", partition.Id);
                npgsqlCommand.Parameters.AddWithValue("type_ids", typeIds.ToArray());
                npgsqlCommand.Parameters.AddWithValue("unique_ids", uniqueIds.ToArray());

                await using NpgsqlDataReader reader = await npgsqlCommand.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    short typeId = reader.GetInt16(0);
                    string uniqueId = reader.GetString(1);

                    // Quick O(1) lookup to find the original object reference
                    if (referenceLookup.TryGetValue((typeId, uniqueId), out IUniqueReference? uniqueReference))
                    {
                        result.Add(uniqueReference);
                    }
                }
            }
            catch (NpgsqlException ex)
            {
                Console.WriteLine($"Postgres Error (ContainsAsync Optimized): {ex.Message}");
                return null;
            }

            return result;
        }
    }
}