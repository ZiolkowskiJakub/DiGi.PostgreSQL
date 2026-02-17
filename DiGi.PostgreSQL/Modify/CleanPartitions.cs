using DiGi.PostgreSQL.Classes;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<List<Partition>?> CleanPartitions(NpgsqlConnection? npgsqlConnection, IEnumerable<short>? partitionIds = null)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            List<Partition>? partitions = await Query.Partitions(npgsqlConnection);
            if (partitions is null)
            {
                return null;
            }

            List<Partition> result = [];
            if (!partitions.Any())
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

                if (!Query.TableExists(npgsqlConnection, tableName))
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

                if(!Query.HasRows(npgsqlConnection, tableName))
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