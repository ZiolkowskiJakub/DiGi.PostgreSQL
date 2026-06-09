using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        /// <summary>
        /// Asynchronously removes a specified database and its associated tablespace from the PostgreSQL server.
        /// </summary>
        /// <param name="connectionData">The connection data used to connect to the PostgreSQL server.</param>
        /// <param name="databaseName">The name of the database to be removed.</param>
        /// <param name="tablespaceName">The name of the tablespace associated with the database to be removed.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the removal was successful; otherwise, false.</returns>
        public static async Task<bool> RemoveDatabaseAsync(this ConnectionData? connectionData, string databaseName, string tablespaceName)
        {
            if (connectionData is null || string.IsNullOrWhiteSpace(databaseName))
            {
                return false;
            }

            ConnectionData connectionData_Temp = new(connectionData, "postgres");

            // Clear pools to ensure C# isn't holding any connections
            NpgsqlConnection.ClearAllPools();

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(connectionData_Temp);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            // 1. Drop Database with FORCE (PG 18 feature)
            await using (NpgsqlCommand npgsqlCommand = new($"DROP DATABASE IF EXISTS \"{databaseName}\" WITH (FORCE)", npgsqlConnection))
            {
                await npgsqlCommand.ExecuteNonQueryAsync();
            }

            // 2. Drop Tablespace
            await RemoveTablespaceAsync(connectionData, tablespaceName);

            return true;
        }
    }
}