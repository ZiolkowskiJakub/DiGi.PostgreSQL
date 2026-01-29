using DiGi.Core.Interfaces;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
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
                short? typeId = await TypeId(npgsqlConnection, keyValuePair.Key);
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

        public static async Task<HashSet<string>?> Contains(this NpgsqlConnection npgsqlConnection, short? typeId, IEnumerable<string>? uniqueIds)
        {
            if (npgsqlConnection is null || typeId is null || uniqueIds is null)
            {
                return null;
            }

            // Query returns the subset of unique_ids that actually exist in the table
            const string commandText = @"
                SELECT unique_id
                FROM objects
                WHERE type_id = @type_id
                  AND unique_id = ANY(@unique_ids)";

            HashSet<string> result = [];

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("type_id", typeId);
            npgsqlCommand.Parameters.AddWithValue("unique_ids", uniqueIds);

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();
            while (await npgsqlDataReader.ReadAsync())
            {
                result.Add(npgsqlDataReader.GetString(0));
            }

            return result;
        }

        public static async Task<bool> Contains(this NpgsqlConnection npgsqlConnection, short? typeId)
        {
            if (npgsqlConnection is null || typeId is null)
            {
                return false;
            }

            await using NpgsqlCommand npgsqlCommand = new("SELECT EXISTS(SELECT 1 FROM objects WHERE type_id = @type_id LIMIT 1)", npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("type_id", typeId);

            var result = await npgsqlCommand.ExecuteScalarAsync();

            return result is bool exists && exists;
        }

        public static async Task<bool> Contains(this NpgsqlConnection npgsqlConnection, Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return false;
            }

            return await Contains(npgsqlConnection, Core.Query.FullTypeName(type));
        }

        public static async Task<bool> Contains(this NpgsqlConnection npgsqlConnection, string? fullTypeName)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(fullTypeName))
            {
                return false;
            }
            short? typeId = await TypeId(npgsqlConnection, fullTypeName);
            if (typeId is null)
            {
                return false;
            }
            return await npgsqlConnection.Contains(typeId);
        }
    }
}