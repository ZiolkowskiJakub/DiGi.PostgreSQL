using DiGi.Core.Interfaces;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<bool> RemoveAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<short>? typeIds)
        {
            if (npgsqlConnection is null || typeIds == null || !typeIds.Any())
            {
                return false;
            }

            // Używamy ANY(@type_ids), co w Postgresie działa jak optymalizowane IN (...)
            const string commandText = "DELETE FROM objects WHERE type_id = ANY(@type_ids);";

            try
            {
                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

                // Przekazujemy całą tablicę jako jeden parametr
                npgsqlCommand.Parameters.AddWithValue("type_ids", typeIds);

                int count = await npgsqlCommand.ExecuteNonQueryAsync();
                if (count > 0)
                {
                    await CleanTypes(npgsqlConnection, typeIds);
                }

                return true;
            }
            catch
            {
                return false;
            }
        }

        public static async Task<List<TUniqueReference>?> RemoveAsync<TUniqueReference>(NpgsqlConnection? npgsqlConnection, IEnumerable<TUniqueReference> uniqueReferences) where TUniqueReference : IUniqueReference
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            // Use RETURNING o.type_id so we know exactly which partition was affected
            string commandText_Delete = @"
                DELETE FROM objects o
                USING types t
                WHERE o.type_id = t.id
                  AND t.full_name = @full_name
                  AND o.unique_id = @unique_id
                RETURNING o.type_id;";

            List<TUniqueReference> result = [];
            HashSet<short> typeIds = [];

            await using NpgsqlCommand npgsqlCommand = new(commandText_Delete, npgsqlConnection);
            npgsqlCommand.Parameters.Add("full_name", NpgsqlDbType.Text);
            npgsqlCommand.Parameters.Add("unique_id", NpgsqlDbType.Text);

            foreach (TUniqueReference uniqueReference in uniqueReferences)
            {
                if (uniqueReference?.TypeReference?.FullTypeName is not string fullTypeName || string.IsNullOrWhiteSpace(uniqueReference.UniqueId))
                {
                    continue;
                }

                npgsqlCommand.Parameters["full_name"].Value = fullTypeName;
                npgsqlCommand.Parameters["unique_id"].Value = uniqueReference.UniqueId;

                if (await npgsqlCommand.ExecuteScalarAsync() is short typeId)
                {
                    result.Add(uniqueReference);
                    typeIds.Add(typeId); // Track this type for cleanup
                }
            }

            await Modify.CleanTypes(npgsqlConnection, typeIds);

            return result;
        }

        public static async Task<bool> RemoveAsync(NpgsqlConnection? npgsqlConnection, Type type, bool inheritance = true)
        {
            if (npgsqlConnection is null || type is null)
            {
                return false;
            }

            IEnumerable<short>? typeIds = null;
            if (!inheritance)
            {
                short? typeId = await Query.TypeId(npgsqlConnection, type);
                if (typeId is not null)
                {
                    typeIds = [typeId.Value];
                }
            }
            else
            {
                Dictionary<short, Type>? dictionary = await Query.TypeIds(npgsqlConnection, type);
                if (dictionary is null || dictionary.Count == 0)
                {
                    return false;
                }
                typeIds = [.. dictionary.Keys];
            }

            if (typeIds is null || !typeIds.Any())
            {
                return false;
            }

            return await RemoveAsync(npgsqlConnection, typeIds);
        }
    }
}