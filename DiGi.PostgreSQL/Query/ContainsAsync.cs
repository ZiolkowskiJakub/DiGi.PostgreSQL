using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously checks which of the specified unique identifiers exist within a partition identified by its ID.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance.</param>
        /// <param name="partitionId">The identifier of the partition to check.</param>
        /// <param name="uniqueIds">The collection of unique identifiers to verify.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a set of existing unique identifiers, or null if any input is null.</returns>
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

        /// <summary>
        /// Asynchronously checks which of the specified unique identifiers exist within the provided partition.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance.</param>
        /// <param name="partition">The partition object to check.</param>
        /// <param name="uniqueIds">The collection of unique identifiers to verify.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a set of existing unique identifiers, or null if any input is null.</returns>
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

        /// <summary>
        /// Asynchronously checks whether any records exist within a partition identified by its ID.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance.</param>
        /// <param name="partitionId">The identifier of the partition to check.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if records exist, otherwise false.</returns>
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

        /// <summary>
        /// Asynchronously checks whether any records exist within a partition identified by its name.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance.</param>
        /// <param name="name">The name of the partition to check.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if records exist, otherwise false.</returns>
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

        /// <summary>
        /// Asynchronously checks whether any records exist within the provided partition.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance.</param>
        /// <param name="partition">The partition object to check.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if records exist, otherwise false.</returns>
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