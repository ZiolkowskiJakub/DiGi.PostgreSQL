using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves the partition ID associated with the specified name.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection used to execute the query.</param>
        /// <param name="name">The name of the partition to retrieve the ID for.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the partition ID as a short if found; otherwise, null.</returns>
        public static async Task<short?> PartitionIdAsync(this NpgsqlConnection? npgsqlConnection, string? name)
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