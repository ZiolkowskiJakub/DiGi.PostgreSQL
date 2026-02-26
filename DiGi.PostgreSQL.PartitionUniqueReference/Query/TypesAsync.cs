using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Query
    {
        public static async Task<List<Classes.Type>?> TypesAsync(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return null;
            }

            string commandText = "SELECT id, name FROM types;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            List<Classes.Type> result = [];

            while (await npgsqlDataReader.ReadAsync())
            {
                string name = npgsqlDataReader.GetString(1);
                if (Core.Query.Type(name, false) is not System.Type type_Temp)
                {
                    continue;
                }

                if (!type.IsAssignableFrom(type_Temp))
                {
                    continue;
                }

                short id = npgsqlDataReader.GetInt16(0);

                result.Add(new Classes.Type(id, name));
            }

            return result;
        }

        public static async Task<List<Classes.Type>?> TypesAsync(this NpgsqlConnection? npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            if (Core.Query.Type(name, false) is not System.Type type)
            {
                return null;
            }

            return await TypesAsync(npgsqlConnection, type);
        }

        public static async Task<List<Classes.Type>?> TypesAsync(this NpgsqlConnection? npgsqlConnection, IEnumerable<short>? typeIds = null)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            string commandText = "SELECT id, name FROM types;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            List<Classes.Type> result = [];

            while (await npgsqlDataReader.ReadAsync())
            {
                short id = npgsqlDataReader.GetInt16(0);
                if (typeIds is not null && !typeIds.Contains(id))
                {
                    continue;
                }

                string name = npgsqlDataReader.GetString(1);
                if (Core.Query.Type(name, false) is not System.Type type_Temp)
                {
                    continue;
                }

                result.Add(new Classes.Type(id, name));
            }

            return result;
        }
    }
}