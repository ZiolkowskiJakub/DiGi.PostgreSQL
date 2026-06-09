using DiGi.Core.Interfaces;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Query
    {
        /// <summary>
        /// Checks whether the specified unique references exist in the database.
        /// </summary>
        /// <typeparam name="TUniqueReference">The type of the unique reference, which must implement <see cref="IUniqueReference"/>.</typeparam>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the query.</param>
        /// <param name="uniqueReferences">A collection of unique references to check for existence.</param>
        /// <returns>A hash set containing the unique references that were found in the database, or null if the connection or input collection is null.</returns>
        public static async Task<HashSet<TUniqueReference>?> ContainsAsync<TUniqueReference>(this NpgsqlConnection npgsqlConnection, IEnumerable<TUniqueReference>? uniqueReferences) where TUniqueReference : IUniqueReference
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
                short? partitionId = await PostgreSQL.Query.PartitionIdAsync(npgsqlConnection, keyValuePair.Key);
                if (partitionId is null)
                {
                    continue;
                }

                HashSet<string>? uniqueIds = await npgsqlConnection.ContainsAsync(partitionId, keyValuePair.Value.Keys);
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

        /// <summary>
        /// Checks whether the specified type exists in the database.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the query.</param>
        /// <param name="type">The type to check for existence.</param>
        /// <returns>A task that represents the asynchronous operation, containing true if the type exists; otherwise, false.</returns>
        public static async Task<bool> ContainsAsync(this NpgsqlConnection npgsqlConnection, Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return false;
            }

            return await PostgreSQL.Query.ContainsAsync(npgsqlConnection, Core.Query.FullTypeName(type));
        }
    }
}