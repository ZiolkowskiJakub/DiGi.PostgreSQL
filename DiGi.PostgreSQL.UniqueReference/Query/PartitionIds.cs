using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Query
    {
        public static async Task<Dictionary<short, System.Type>?> PartitionIds(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            string commandText = "SELECT id, name FROM partitions;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            Dictionary<short, System.Type> result = [];

            while (await npgsqlDataReader.ReadAsync())
            {
                string name = npgsqlDataReader.GetString(1);
                if (Core.Query.Type(name, false) is not System.Type type_Temp)
                {
                    continue;
                }

                result[npgsqlDataReader.GetInt16(0)] = type_Temp;
            }

            return result;
        }

        public static async Task<Dictionary<short, System.Type>?> PartitionIds(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return null;
            }

            string commandText = "SELECT id, name FROM partitions;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            Dictionary<short, System.Type> result = [];

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

                result[npgsqlDataReader.GetInt16(0)] = type_Temp;
            }

            return result;
        }

        public static async Task<Dictionary<short, System.Type>?> PartitionIds(this NpgsqlConnection? npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            if (Core.Query.Type(name, false) is not System.Type type)
            {
                return null;
            }

            return await PartitionIds(npgsqlConnection, type);
        }
    }
}