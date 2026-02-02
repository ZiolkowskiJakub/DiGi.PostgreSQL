using DiGi.Core.Interfaces;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Query
    {
        public static async Task<HashSet<TUniqueReference>?> Contains<TUniqueReference>(this NpgsqlConnection npgsqlConnection, IEnumerable<TUniqueReference>? uniqueReferences) where TUniqueReference : IUniqueReference
        {
            if (npgsqlConnection is null || uniqueReferences is null)
            {
                return null;
            }

            Dictionary<string, Dictionary<string, TUniqueReference>> dictionary = [];
            foreach (TUniqueReference uniqueReference in uniqueReferences)
            {
                string? fullTypeName = uniqueReference.TypeReference?.FullTypeName;
                if (string.IsNullOrWhiteSpace(fullTypeName))
                {
                    continue;
                }

                string? uniqueId = uniqueReference.UniqueId;
                if (string.IsNullOrWhiteSpace(uniqueId))
                {
                    continue;
                }

                if (dictionary.TryGetValue(fullTypeName, out Dictionary<string, TUniqueReference>? dictionary_UniqueId) || dictionary_UniqueId is null)
                {
                    dictionary_UniqueId = [];
                    dictionary[fullTypeName] = dictionary_UniqueId;
                }

                dictionary_UniqueId[uniqueId] = uniqueReference;
            }

            HashSet<TUniqueReference> result = [];

            foreach (KeyValuePair<string, Dictionary<string, TUniqueReference>> keyValuePair in dictionary)
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
                    if (keyValuePair.Value.TryGetValue(uniqueId, out TUniqueReference? uniqueReference) && uniqueReference is not null)
                    {
                        result.Add(uniqueReference);
                    }
                }
            }

            return result;
        }

        public static async Task<bool> Contains(this NpgsqlConnection npgsqlConnection, Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return false;
            }

            return await PostgreSQL.Query.Contains(npgsqlConnection, Core.Query.FullTypeName(type));
        }
    }
}