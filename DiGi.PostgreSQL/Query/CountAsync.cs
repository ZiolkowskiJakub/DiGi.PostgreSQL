using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<long> CountAsync(this NpgsqlConnection npgsqlConnection, IEnumerable<short> partitionIds)
        {
            if (npgsqlConnection is null || partitionIds is null)
            {
                return -1;
            }

            if (!partitionIds.Any())
            {
                return -1;
            }

            List<Partition>? partitions = await Partitions(npgsqlConnection, partitionIds);
            if (partitions is null || partitions.Count == 0)
            {
                return -1;
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

            long result = 0;
            foreach (KeyValuePair<Enums.DataType, List<Partition>> keyValuePair in dictionary)
            {
                // Summing up everything that matches any ID in the provided array
                string commandText = $"SELECT COUNT(*) FROM objects_{(int)keyValuePair.Key} WHERE partition_id = ANY(@partition_ids)";

                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                npgsqlCommand.Parameters.AddWithValue("partition_ids", partitionIds);

                // If no rows match, PostgreSQL returns 0; ExecuteScalar returns long for COUNT
                var @var = await npgsqlCommand.ExecuteScalarAsync();
                if (@var is long count)
                {
                    result += count;
                }
            }

            return result;
        }
    }
}