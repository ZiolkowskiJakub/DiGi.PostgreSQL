using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<short?> UpdateTypeIdAsync(this NpgsqlConnection? npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            short? partitionId = await Query.PartitionId(npgsqlConnection, name);
            if (partitionId is not null)
            {
                return partitionId;
            }

            string commandText = @"
                INSERT INTO partitions (name)
                VALUES (@name)
                RETURNING id;
                ";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("name", name);

            partitionId = (short?)await npgsqlCommand.ExecuteScalarAsync();
            if (partitionId is not null)
            {
                await Create.Table_Objects_Partition(npgsqlConnection, partitionId.Value);
            }

            return partitionId;
        }
    }
}