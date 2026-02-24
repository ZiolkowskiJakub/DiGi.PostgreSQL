using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.Enums;
using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<bool> RemoveAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<short>? partitionIds)
        {
            if (npgsqlConnection is null || partitionIds == null || !partitionIds.Any())
            {
                return false;
            }

            List<Partition>? partitions = await Query.PartitionsAsync(npgsqlConnection);
            if (partitions is null || partitions.Count == 0)
            {
                return false;
            }

            Dictionary<DataType, HashSet<short>> dictionary = [];
            foreach (Partition partition in partitions)
            {
                if (partition is null || !partitionIds.Contains(partition.Id))
                {
                    continue;
                }

                if (!dictionary.TryGetValue(partition.DataType, out HashSet<short>? partitionIds_Temp) || partitionIds_Temp == null)
                {
                    partitionIds_Temp = [];
                    dictionary[partition.DataType] = partitionIds_Temp;
                }

                partitionIds_Temp.Add(partition.Id);
            }

            if (dictionary.Count == 0)
            {
                return false;
            }

            foreach (KeyValuePair<DataType, HashSet<short>> keyValuePair in dictionary)
            {
                string tableName = $"objects_{(int)keyValuePair.Key}";

                if (!Query.TableExists(npgsqlConnection, tableName))
                {
                    continue;
                }

                await using NpgsqlCommand npgsqlCommand = new($"DELETE FROM {tableName} WHERE partition_id = ANY(@partition_ids);", npgsqlConnection);

                // Przekazujemy całą tablicę jako jeden parametr
                npgsqlCommand.Parameters.AddWithValue("partition_ids", keyValuePair.Value.ToArray());

                int count = await npgsqlCommand.ExecuteNonQueryAsync();
                if (count > 0)
                {
                    await CleanPartitionsAsync(npgsqlConnection, partitionIds);
                }

                return true;
            }

            return false;
        }
    }
}