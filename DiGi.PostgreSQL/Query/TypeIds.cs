using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public async static Task<Dictionary<short, System.Type>?> TypeIds(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return null;
            }

            string commandText = "SELECT id, full_name FROM object_types;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            Dictionary<short, System.Type> result = [];

            while (await npgsqlDataReader.ReadAsync())
            {
                string fullName_Temp = npgsqlDataReader.GetString(1);
                if (Core.Query.Type(fullName_Temp, false) is not System.Type type_Temp)
                {
                    continue;
                }

                if (!type.IsAssignableFrom(type_Temp))
                {
                    continue;
                }

                result[npgsqlDataReader.GetInt16(0)] = type_Temp;
            }

            return result;
        }

        public async static Task<Dictionary<short, System.Type>?> TypeIds(this NpgsqlConnection? npgsqlConnection, string? fullName)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(fullName))
            {
                return null;
            }

            if (Core.Query.Type(fullName, false) is not System.Type type)
            {
                return null;
            }

            return await TypeIds(npgsqlConnection, type);
        }
    }
}