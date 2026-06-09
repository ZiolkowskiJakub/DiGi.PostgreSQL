using DiGi.PostgreSQL.Classes;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        /// <summary>
        /// Asynchronously cleans up partitions by removing empty ones from the metadata and dropping physical tables if they contain no rows.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection to be used for the cleanup process.</param>
        /// <returns>A list of partitions that were removed, or <see langword="null"/> if the provided connection is null or partition data could not be retrieved.</returns>
        public static async Task<List<Partition>?> CleanPartitionsAsync(NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            List<Partition>? partitions = await Query.PartitionsAsync(npgsqlConnection);
            if (partitions is null)
            {
                return null;
            }

            List<Partition> result = [];
            if (partitions.Count == 0)
            {
                return result;
            }

            Dictionary<Enums.DataType, List<Partition>> dictionary = [];
            foreach (Partition partition in partitions)
            {
                if (!dictionary.TryGetValue(partition.DataType, out List<Partition>? partitions_Id) || partitions_Id is null)
                {
                    partitions_Id = [];
                    dictionary[partition.DataType] = partitions_Id;
                }

                partitions_Id.Add(partition);
            }

            foreach (KeyValuePair<Enums.DataType, List<Partition>> keyValuePair in dictionary)
            {
                string tableName = $"objects_{(int)keyValuePair.Key}";

                if (!await Query.TableExistsAsync(npgsqlConnection, tableName))
                {
                    result.AddRange(keyValuePair.Value);
                    continue;
                }

                foreach (Partition partition in keyValuePair.Value)
                {
                    // Check if the partition is now empty
                    await using NpgsqlCommand npgsqlCommand_Check = new($"SELECT NOT EXISTS(SELECT 1 FROM {tableName} WHERE partition_id = @partition_id);", npgsqlConnection);
                    npgsqlCommand_Check.Parameters.Add("partition_id", NpgsqlDbType.Smallint).Value = partition.Id;

                    bool notContains = (bool)(await npgsqlCommand_Check.ExecuteScalarAsync() ?? false);
                    if (notContains)
                    {
                        // 1. Remove from types first (Metadata)
                        await using NpgsqlCommand npgsqlCommand_DeleteType = new("DELETE FROM partitions WHERE id = @partition_id;", npgsqlConnection);
                        npgsqlCommand_DeleteType.Parameters.Add("partition_id", NpgsqlDbType.Smallint).Value = partition.Id;
                        await npgsqlCommand_DeleteType.ExecuteNonQueryAsync();

                        result.Add(partition);
                    }
                }

                if (!Query.HasRows(npgsqlConnection, tableName))
                {
                    // 2. Drop the physical partition table
                    await using NpgsqlCommand npgsqlCommand_DropTable = new($"DROP TABLE IF EXISTS {tableName};", npgsqlConnection);
                    await npgsqlCommand_DropTable.ExecuteNonQueryAsync();
                }
            }

            return result;
        }
    }
}