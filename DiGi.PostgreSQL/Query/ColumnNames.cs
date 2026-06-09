using Npgsql;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves the column names for a specified table from the PostgreSQL database.
        /// </summary>
        /// <param name="npgsqlConnection">The <see cref="NpgsqlConnection"/> used to connect to the database.</param>
        /// <param name="tableName">The name of the table to retrieve columns for.</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/> to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of column names in lowercase, or null if the connection is null or the table name is null or whitespace.</returns>
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