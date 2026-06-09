using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        /// <summary>
        /// Asynchronously removes a table from the database if it exists.
        /// </summary>
        /// <param name="connectionData">The connection data used to establish the database connection.</param>
        /// <param name="tableName">The name of the table to be removed.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains true if the table was successfully removed or did not exist; otherwise, false.</returns>
        public static async Task<bool> RemoveTableAsync(this ConnectionData? connectionData, string tableName)
        {
            if (connectionData is null || string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            // Clear pools to ensure C# isn't holding any connections
            NpgsqlConnection.ClearAllPools();

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(connectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            await using (NpgsqlCommand npgsqlCommand = new($"DROP TABLE IF EXISTS {tableName}", npgsqlConnection))
            {
                await npgsqlCommand.ExecuteNonQueryAsync();
            }

            return true;
        }
    }
}