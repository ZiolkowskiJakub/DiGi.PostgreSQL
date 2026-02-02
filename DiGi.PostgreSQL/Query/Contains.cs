using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<HashSet<string>?> Contains(this NpgsqlConnection npgsqlConnection, short? partitionId, IEnumerable<string>? uniqueIds)
        {
            if (npgsqlConnection is null || partitionId is null || uniqueIds is null)
            {
                return null;
            }

            // Query returns the subset of unique_ids that actually exist in the table
            const string commandText = @"
                SELECT unique_id
                FROM objects
                WHERE partition_id = @partition_id
                  AND unique_id = ANY(@unique_ids)";

            HashSet<string> result = [];

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("partition_id", partitionId);
            npgsqlCommand.Parameters.AddWithValue("unique_ids", uniqueIds);

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();
            while (await npgsqlDataReader.ReadAsync())
            {
                result.Add(npgsqlDataReader.GetString(0));
            }

            return result;
        }

        public static async Task<bool> Contains(this NpgsqlConnection npgsqlConnection, short? partitionId)
        {
            if (npgsqlConnection is null || partitionId is null)
            {
                return false;
            }

            await using NpgsqlCommand npgsqlCommand = new("SELECT EXISTS(SELECT 1 FROM objects WHERE partition_id = @partition_id LIMIT 1)", npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("partition_id", partitionId);

            var result = await npgsqlCommand.ExecuteScalarAsync();

            return result is bool exists && exists;
        }

        public static async Task<bool> Contains(this NpgsqlConnection npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return false;
            }
            short? partitionId = await PartitionId(npgsqlConnection, name);
            if (partitionId is null)
            {
                return false;
            }
            return await npgsqlConnection.Contains(partitionId);
        }
    }
}