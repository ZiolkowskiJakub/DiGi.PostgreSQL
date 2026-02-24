using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Query
    {
        public static async Task<List<Partition>?> PartitionsAsync(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return null;
            }

            string commandText = "SELECT id, name, data_type FROM partitions;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            List<Partition> result = [];

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
                short dataType = npgsqlDataReader.GetInt16(2);

                result.Add(new Partition(id, name, (Enums.DataType)dataType));
            }

            return result;
        }

        public static async Task<List<Partition>?> PartitionsAsync(this NpgsqlConnection? npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            if (Core.Query.Type(name, false) is not System.Type type)
            {
                return null;
            }

            return await PartitionsAsync(npgsqlConnection, type);
        }
    }
}