using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<long> CountAsync(this NpgsqlConnection npgsqlConnection, Type? type, bool inheritance = true)
        {
            if (npgsqlConnection is null || type is null)
            {
                return -1;
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
                    return 0;
                }
                typeIds = [.. dictionary.Keys];
            }

            if (typeIds is null || !typeIds.Any())
            {
                return 0;
            }

            return await npgsqlConnection.CountAsync(typeIds);
        }

        public static async Task<long> CountAsync(this NpgsqlConnection npgsqlConnection, IEnumerable<short> typeIds)
        {
            if (npgsqlConnection is null || typeIds is null)
            {
                return -1;
            }

            if (!typeIds.Any())
            {
                return 0;
            }

            // Summing up everything that matches any ID in the provided array
            const string commandText = "SELECT COUNT(*) FROM objects WHERE type_id = ANY(@type_ids)";

            await using NpgsqlCommand npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("type_ids", typeIds);

            var result = await npgsqlCommand.ExecuteScalarAsync();

            // If no rows match, PostgreSQL returns 0; ExecuteScalar returns long for COUNT
            return result is long count ? count : 0;
        }
    }
}