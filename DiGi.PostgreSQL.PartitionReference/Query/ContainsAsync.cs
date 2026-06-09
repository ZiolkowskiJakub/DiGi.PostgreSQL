using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionReference
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously checks if the specified partition references exist in the database.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to be used for the operation.</param>
        /// <param name="partitionReferences">The collection of partition references to check for existence.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a HashSet of the partition references that exist in the database, or null if the connection or the input collection is null.</returns>
        public static async Task<HashSet<Classes.PartitionReference>?> ContainsAsync(this NpgsqlConnection npgsqlConnection, IEnumerable<Classes.PartitionReference>? partitionReferences)
        {
            if (npgsqlConnection is null || partitionReferences is null)
            {
                return null;
            }

            Dictionary<string, Dictionary<string, Classes.PartitionReference>> dictionary = [];
            foreach (Classes.PartitionReference partitionReference in partitionReferences)
            {
                string? name = partitionReference.Name;
                if (string.IsNullOrWhiteSpace(name))
                {
                    continue;
                }

                string? uniqueId = partitionReference.UniqueId;
                if (string.IsNullOrWhiteSpace(uniqueId))
                {
                    continue;
                }

                if (dictionary.TryGetValue(name, out Dictionary<string, Classes.PartitionReference>? dictionary_UniqueId) || dictionary_UniqueId is null)
                {
                    dictionary_UniqueId = [];
                    dictionary[name] = dictionary_UniqueId;
                }

                dictionary_UniqueId[uniqueId] = partitionReference;
            }

            HashSet<Classes.PartitionReference> result = [];

            foreach (KeyValuePair<string, Dictionary<string, Classes.PartitionReference>> keyValuePair in dictionary)
            {
                short? typeId = await PostgreSQL.Query.PartitionIdAsync(npgsqlConnection, keyValuePair.Key);
                if (typeId is null)
                {
                    continue;
                }

                HashSet<string>? uniqueIds = await npgsqlConnection.ContainsAsync(typeId, keyValuePair.Value.Keys);
                if (uniqueIds is null || uniqueIds.Count == 0)
                {
                    continue;
                }

                foreach (string uniqueId in uniqueIds)
                {
                    if (keyValuePair.Value.TryGetValue(uniqueId, out Classes.PartitionReference? partitionReference) && partitionReference is not null)
                    {
                        result.Add(partitionReference);
                    }
                }
            }

            return result;
        }
    }
}