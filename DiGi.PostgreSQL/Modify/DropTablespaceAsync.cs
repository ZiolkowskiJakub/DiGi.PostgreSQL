using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<bool> DropTablespaceAsync(this ConnectionData? connectionData, string tablespaceName)
        {
            if (connectionData is null || string.IsNullOrWhiteSpace(tablespaceName))
            {
                return false;
            }

            ConnectionData connectionData_Temp = new(connectionData, "postgres");

            // Connect to the default 'postgres' database to execute admin commands
            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(connectionData_Temp);
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
                await using NpgsqlCommand npgsqlCommand_Drop = new NpgsqlCommand($"DROP TABLESPACE IF EXISTS {tablespaceName}", npgsqlConnection);
                await npgsqlCommand_Drop.ExecuteNonQueryAsync();

                return true;
            }

            return false;
        }
    }
}