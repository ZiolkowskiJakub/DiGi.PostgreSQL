using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Query
    {
        public static async Task<short?> TypeIdAsync(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null || Core.Query.FullTypeName(type) is not string name)
            {
                return null;
            }

            string commandText = @"
                SELECT id
                FROM types
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