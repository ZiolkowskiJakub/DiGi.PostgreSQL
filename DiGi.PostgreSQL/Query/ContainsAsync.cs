using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<HashSet<string>?> ContainsAsync(this NpgsqlConnection npgsqlConnection, short? partitionId, IEnumerable<string>? uniqueIds)
        {
            if (npgsqlConnection is null || partitionId is null || uniqueIds is null)
            {
                return null;
            }

            if (!uniqueIds.Any())
            {
                return [];
            }

            Classes.Partition? partition = await PartitionAsync(npgsqlConnection, partitionId.Value);
            if (partition is null)
            {
                return null;
            }

            return await ContainsAsync(npgsqlConnection, partition, uniqueIds);
        }

        public static async Task<HashSet<string>?> ContainsAsync(this NpgsqlConnection npgsqlConnection, Classes.Partition? partition, IEnumerable<string>? uniqueIds)
        {
            if (npgsqlConnection is null || partition is null || uniqueIds is null)
            {
                return null;
            }

            // Query returns the subset of unique_ids that actually exist in the table
            string commandText = $@"
                SELECT unique_id
                FROM objects_{(int)partition.DataType}
                WHERE partition_id = @partition_id
                  AND unique_id = ANY(@unique_ids)";

            HashSet<string> result = [];

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("partition_id", partition.Id);
            npgsqlCommand.Parameters.AddWithValue("unique_ids", uniqueIds);

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();
            while (await npgsqlDataReader.ReadAsync())
            {
                result.Add(npgsqlDataReader.GetString(0));
            }

            return result;
        }

        public static async Task<bool> ContainsAsync(this NpgsqlConnection npgsqlConnection, short? partitionId)
        {
            if (npgsqlConnection is null || partitionId is null)
            {
                return false;
            }

            Classes.Partition? partition = await PartitionAsync(npgsqlConnection, partitionId.Value);
            if (partition is null)
            {
                return false;
            }

            return await npgsqlConnection.ContainsAsync(partition);
        }

        public static async Task<bool> ContainsAsync(this NpgsqlConnection npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return false;
            }

            Classes.Partition? partition = await PartitionAsync(npgsqlConnection, name);
            if (partition is null)
            {
                return false;
            }

            return await npgsqlConnection.ContainsAsync(partition);
        }

        public static async Task<bool> ContainsAsync(this NpgsqlConnection npgsqlConnection, Classes.Partition partition)
        {
            if (npgsqlConnection is null || partition is null)
            {
                return false;
            }

            await using NpgsqlCommand npgsqlCommand = new($"SELECT EXISTS(SELECT 1 FROM objects_{(int)partition.DataType} WHERE partition_id = @partition_id LIMIT 1)", npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("partition_id", partition.Id);

            var result = await npgsqlCommand.ExecuteScalarAsync();

            return result is bool exists && exists;
        }
    }
}