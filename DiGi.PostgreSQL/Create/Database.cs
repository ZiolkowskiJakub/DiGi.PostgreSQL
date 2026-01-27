using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        public static async Task<bool> Database(ConnectionData? connectionData, string databaseName)
        {
            if (connectionData is null || string.IsNullOrWhiteSpace(databaseName))
            {
                return false;
            }

            ConnectionData connectionData_Temp = new(connectionData, "postgres");

            // Connect to the default 'postgres' database to execute admin commands
            await using NpgsqlConnection? npgsqlConnection = NpgsqlConnection(connectionData_Temp);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            // Check if the database already exists to avoid errors
            await using (NpgsqlCommand npgsqlCommand_Select = new($"SELECT 1 FROM pg_database WHERE datname = '{databaseName}'", npgsqlConnection))
            {
                object? result = await npgsqlCommand_Select.ExecuteScalarAsync();
                if (result != null)
                {
                    return true;
                }
            }

            // Create the database
            await using NpgsqlCommand npgsqlCommand_Create = new NpgsqlCommand($"CREATE DATABASE \"{databaseName}\"", npgsqlConnection);
            await npgsqlCommand_Create.ExecuteNonQueryAsync();

            return true;
        }

        public static async Task<bool> Database(this ConnectionData? connectionData, string databaseName, string tablespaceName, string directory)
        {
            if (connectionData is null || string.IsNullOrWhiteSpace(databaseName))
            {
                return false;
            }

            ConnectionData connectionData_Temp = new(connectionData, "postgres");

            // Connect to the default 'postgres' database to execute admin commands
            await using NpgsqlConnection? npgsqlConnection = NpgsqlConnection(connectionData_Temp);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            // 1. Check if Tablespace exists
            bool tablespaceExists;
            await using (NpgsqlCommand npgsqlCommand_Select = new("SELECT 1 FROM pg_tablespace WHERE spcname = @tablespaceName", npgsqlConnection))
            {
                npgsqlCommand_Select.Parameters.AddWithValue("tablespaceName", tablespaceName);
                tablespaceExists = (await npgsqlCommand_Select.ExecuteScalarAsync()) != null;
            }

            if (!tablespaceExists)
            {
                // Identifiers (tablespace names) can't be parameterized, so we sanitize
                await using NpgsqlCommand NpgsqlCommand_CreateTablespace = new($"CREATE TABLESPACE {tablespaceName} LOCATION '{directory}'", npgsqlConnection);
                await NpgsqlCommand_CreateTablespace.ExecuteNonQueryAsync();
            }

            // 2. Check if Database exists
            bool databaseExists;
            await using (NpgsqlCommand NpgsqlCommand_Select = new("SELECT 1 FROM pg_database WHERE datname = @databaseName", npgsqlConnection))
            {
                NpgsqlCommand_Select.Parameters.AddWithValue("databaseName", databaseName);
                databaseExists = (await NpgsqlCommand_Select.ExecuteScalarAsync()) != null;
            }

            if (!databaseExists)
            {
                await using NpgsqlCommand npgsqlCommand_Create = new($"CREATE DATABASE \"{databaseName}\" TABLESPACE = {tablespaceName}", npgsqlConnection);
                await npgsqlCommand_Create.ExecuteNonQueryAsync();
            }

            return true;
        }
    }
}