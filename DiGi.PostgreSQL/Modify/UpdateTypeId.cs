using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public async static Task<short?> UpdateTypeId(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null || Core.Query.FullTypeName(type) is not string fullName)
            {
                return null;
            }

            return await UpdateTypeId(npgsqlConnection, fullName);
        }

        public async static Task<short?> UpdateTypeId(this NpgsqlConnection? npgsqlConnection, string? fullName)
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
                INSERT INTO object_types (code, description)
                VALUES (@code, @desc)
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