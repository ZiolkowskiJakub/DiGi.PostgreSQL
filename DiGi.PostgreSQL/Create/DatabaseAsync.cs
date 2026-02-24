using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        public static async Task<bool> DatabaseAsync(ConnectionData? connectionData)
        {
            if (connectionData is null || string.IsNullOrWhiteSpace(connectionData.Database))
            {
                return false;
            }

            ConnectionData connectionData_Temp = connectionData.GetDefault();

            // Connect to the default 'postgres' database to execute admin commands
            await using NpgsqlConnection? npgsqlConnection = NpgsqlConnection(connectionData_Temp);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            // Check if the database already exists to avoid errors
            await using (NpgsqlCommand npgsqlCommand_Select = new($"SELECT 1 FROM pg_database WHERE datname = '{connectionData.Database}'", npgsqlConnection))
            {
                object? result = await npgsqlCommand_Select.ExecuteScalarAsync();
                if (result != null)
                {
                    return true;
                }
            }

            // Create the database
            await using NpgsqlCommand npgsqlCommand_Create = new($"CREATE DATABASE \"{connectionData.Database}\"", npgsqlConnection);
            await npgsqlCommand_Create.ExecuteNonQueryAsync();

            return true;
        }

        public static async Task<bool> DatabaseAsync(this PostgreSQLConfigurationFile? postgreSQLConfigurationFile)
        {
            if (postgreSQLConfigurationFile is null)
            {
                return false;
            }

            if (postgreSQLConfigurationFile.Tablespace is not string tablespace || string.IsNullOrWhiteSpace(tablespace) || postgreSQLConfigurationFile.Directory is not string directory || string.IsNullOrWhiteSpace(directory))
            {
                return false;
            }

            if (!System.IO.Directory.Exists(directory))
            {
                return false;
            }

            return await DatabaseAsync(ConnectionData(postgreSQLConfigurationFile), tablespace, directory);
        }

        public static async Task<bool> DatabaseAsync(this ConnectionData? connectionData, string tablespaceName, string directory)
        {
            if (connectionData is null)
            {
                return false;
            }

            if (connectionData.Database is not string databaseName)
            {
                return false;
            }

            ConnectionData connectionData_Default = connectionData.GetDefault();

            // Connect to the default 'postgres' database to execute admin commands
            await using NpgsqlConnection? npgsqlConnection = NpgsqlConnection(connectionData_Default);
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