using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<short?> PartitionId(this NpgsqlConnection? npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            string commandText = @"
                SELECT id
                FROM partitions
                WHERE name = @name;
                ";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

            npgsqlCommand.Parameters.AddWithValue("name", name);

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();
            if (await npgsqlDataReader.ReadAsync())
            {
                return npgsqlDataReader.GetInt16(0);
            }

            return null;
        }
    }
}