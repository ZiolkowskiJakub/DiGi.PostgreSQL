using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        /// <summary>
        /// Asynchronously removes the specified tablespace from the PostgreSQL server if it is not currently in use by any database.
        /// </summary>
        /// <param name="connectionData">The connection data used to establish a connection to the PostgreSQL server.</param>
        /// <param name="tablespaceName">The name of the tablespace to be removed.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is <c>true</c> if the tablespace was successfully removed or did not exist; otherwise, <c>false</c>.</returns>
        public static async Task<bool> RemoveTablespaceAsync(this ConnectionData? connectionData, string tablespaceName)
        {
            if (connectionData is null || string.IsNullOrWhiteSpace(tablespaceName))
            {
                return false;
            }

            ConnectionData connectionData_Default = connectionData.GetDefault();

            // Connect to the default 'postgres' database to execute admin commands
            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(connectionData_Default);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            // 1. Check if any databases are still using this tablespace
            var commandText = @"
                SELECT COUNT(*)
                FROM pg_database db
                JOIN pg_tablespace ts ON db.dattablespace = ts.oid
                WHERE ts.spcname = @tablespaceName";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("tablespaceName", tablespaceName);

            int count = System.Convert.ToInt32(await npgsqlCommand.ExecuteScalarAsync());

            if (count == 0)
            {
                // 2. Only drop if it's truly empty
                await using NpgsqlCommand npgsqlCommand_Drop = new($"DROP TABLESPACE IF EXISTS {tablespaceName}", npgsqlConnection);
                await npgsqlCommand_Drop.ExecuteNonQueryAsync();

                return true;
            }

            return false;
        }
    }
}