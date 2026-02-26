using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Modify
    {
        public static async Task<Classes.Type?> UpdateTypeIdAsync(this NpgsqlConnection? npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            Classes.Type? result = await Query.TypeAsync(npgsqlConnection, name);
            if (result is null)
            {
                string commandText = @"
                INSERT INTO types (name)
                VALUES (@name)
                RETURNING id;";

                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                npgsqlCommand.Parameters.AddWithValue("name", name);

                short? typeId = (short?)await npgsqlCommand.ExecuteScalarAsync();
                if (typeId is null)
                {
                    return null;
                }

                result = await Query.TypeAsync(npgsqlConnection, name);
            }

            return result;
        }
    }
}