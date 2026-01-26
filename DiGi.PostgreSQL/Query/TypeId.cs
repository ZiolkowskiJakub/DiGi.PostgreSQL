using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public async static Task<short?> TypeId(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null || Core.Query.FullTypeName(type) is not string fullName)
            {
                return null;
            }

            return await TypeId(npgsqlConnection, fullName);
        }

        public async static Task<short?> TypeId(this NpgsqlConnection? npgsqlConnection, string? fullName)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(fullName))
            {
                return null;
            }

            string commandText = @"
                SELECT id
                FROM object_types
                WHERE full_name = @full_name;
                ";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

            npgsqlCommand.Parameters.AddWithValue("full_name", fullName);

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();
            if (await npgsqlDataReader.ReadAsync())
            {
                return npgsqlDataReader.GetInt16(0);
            }

            return null;
        }
    }
}