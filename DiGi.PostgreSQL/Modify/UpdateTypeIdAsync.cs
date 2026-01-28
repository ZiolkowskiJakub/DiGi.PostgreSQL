using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<short?> UpdateTypeIdAsync(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null || Core.Query.FullTypeName(type) is not string fullName)
            {
                return null;
            }

            return await UpdateTypeIdAsync(npgsqlConnection, fullName);
        }

        public static async Task<short?> UpdateTypeIdAsync(this NpgsqlConnection? npgsqlConnection, string? fullName)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(fullName))
            {
                return null;
            }

            short? typeId = await Query.TypeId(npgsqlConnection, fullName);
            if (typeId is not null)
            {
                return typeId;
            }

            string commandText = @"
                INSERT INTO types (full_name)
                VALUES (@full_name)
                RETURNING id;
                ";

            await using var cmd = new NpgsqlCommand(commandText, npgsqlConnection);
            cmd.Parameters.AddWithValue("full_name", fullName);

            typeId = (short?)await cmd.ExecuteScalarAsync();
            if (typeId is not null)
            {
                await Create.Table_Objects_Partition(npgsqlConnection, typeId.Value);
            }

            return typeId;
        }
    }
}