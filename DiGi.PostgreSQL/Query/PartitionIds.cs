using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<Dictionary<short, string>?> PartitionIds(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            string commandText = "SELECT id, name FROM partitions;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            Dictionary<short, string> result = [];

            while (await npgsqlDataReader.ReadAsync())
            {
                result[npgsqlDataReader.GetInt16(0)] = npgsqlDataReader.GetString(1);
            }

            return result;
        }
    }
}