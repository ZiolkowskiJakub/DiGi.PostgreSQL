using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<Classes.Partition?> UpdatePartitionIdAsync(this NpgsqlConnection? npgsqlConnection, string? name, Enums.DataType dataType)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            Classes.Partition? partition = await Query.PartitionAsync(npgsqlConnection, name);
            if (partition is null)
            {
                string commandText = @"
                INSERT INTO partitions (name, data_type)
                VALUES (@name, @data_type)
                RETURNING id;";

                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                npgsqlCommand.Parameters.AddWithValue("name", name);
                npgsqlCommand.Parameters.AddWithValue("data_type", (short)dataType);

                short? partitionId = (short?)await npgsqlCommand.ExecuteScalarAsync();
                if (partitionId is null)
                {
                    return null;
                }

                partition = await Query.PartitionAsync(npgsqlConnection, name);
            }

            if (partition is null)
            {
                return null;
            }

            bool created = await Create.TableAsync_Objects_Partition(npgsqlConnection, dataType, partition.Id);
            if (created)
            {
                return partition;
            }

            return null;
        }
    }
}