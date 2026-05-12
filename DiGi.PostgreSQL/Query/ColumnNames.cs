using Npgsql;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<List<string>?> ColumnNamesAsync(this NpgsqlConnection? npgsqlConnection, string? tableName, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName))
            {
                return null;
            }

            List<string> result = [];

            const string commandText = @"
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = @tableName;";

            using (NpgsqlCommand command = new(commandText, npgsqlConnection))
            {
                command.Parameters.AddWithValue("tableName", tableName);

                using NpgsqlDataReader npgsqlDataReader = await command.ExecuteReaderAsync(cancellationToken);

                while (await npgsqlDataReader.ReadAsync())
                {
                    result.Add(npgsqlDataReader.GetString(0).ToLower());
                }
            }
            return result;
        }
    }
}