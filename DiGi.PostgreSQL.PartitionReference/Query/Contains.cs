using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionReference
{
    public static partial class Query
    {
        public static async Task<HashSet<Classes.PartitionReference>?> Contains(this NpgsqlConnection npgsqlConnection, IEnumerable<Classes.PartitionReference>? partitionReferences)
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
                short? typeId = await PostgreSQL.Query.PartitionId(npgsqlConnection, keyValuePair.Key);
                if (typeId is null)
                {
                    continue;
                }

                HashSet<string>? uniqueIds = await npgsqlConnection.Contains(typeId, keyValuePair.Value.Keys);
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