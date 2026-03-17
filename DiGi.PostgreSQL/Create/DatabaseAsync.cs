using DiGi.PostgreSQL.Classes;
using Npgsql;
using System;
using System.IO;
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

            // Establish connection to the 'postgres' maintenance database
            await using NpgsqlConnection? npgsqlConnection = NpgsqlConnection(connectionData_Temp);
            if(npgsqlConnection is null)
            {
                return false;
            }
            
            await npgsqlConnection.OpenAsync();

            // Check if database exists using parameters to prevent SQL Injection
            string commandText_Select = "SELECT 1 FROM pg_database WHERE datname = @databaseName";
            await using (NpgsqlCommand npgsqlCommand_Select = new (commandText_Select, npgsqlConnection))
            {
                npgsqlCommand_Select.Parameters.AddWithValue("databaseName", connectionData.Database);
                object? result = await npgsqlCommand_Select.ExecuteScalarAsync();
                if (result != null)
                {
                    return true; // Database already exists
                }
            }

            // Create database - Note: Identifiers cannot be parameterized. 
            // We use quoted identifiers for safety.
            string commandText_Create = $"CREATE DATABASE \"{connectionData.Database.Replace("\"", "\"\"")}\"";
            
            await using NpgsqlCommand npgsqlCommand_Create = new(commandText_Create, npgsqlConnection);
            
            await npgsqlCommand_Create.ExecuteNonQueryAsync();

            return true;
        }

        public static async Task<bool> DatabaseAsync(this ConnectionData? connectionData, string? tablespaceName = null, string? directory = null)
        {
            if (connectionData is null || string.IsNullOrWhiteSpace(connectionData.Database))
            {
                return false;
            }

            if (!string.IsNullOrWhiteSpace(directory) && !Directory.Exists(directory))
            {
                return false;
            }

            ConnectionData connectionData_Temp = connectionData.GetDefault();

            await using NpgsqlConnection? npgsqlConnection = NpgsqlConnection(connectionData_Temp);
            if(npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            // 1. Handle Tablespace if provided
            bool useTablespace = !string.IsNullOrWhiteSpace(tablespaceName) && !string.IsNullOrWhiteSpace(directory);

            if (useTablespace)
            {
                bool tablespaceExists = false;
                string commandText_SelectTablespace = "SELECT 1 FROM pg_tablespace WHERE spcname = @tablespaceName";

                await using (NpgsqlCommand npgsqlCommand_Select = new NpgsqlCommand(commandText_SelectTablespace, npgsqlConnection))
                {
                    npgsqlCommand_Select.Parameters.AddWithValue("tablespaceName", tablespaceName!);
                    tablespaceExists = (await npgsqlCommand_Select.ExecuteScalarAsync()) != null;
                }

                if (!tablespaceExists)
                {
                    string commandText_CreateTablespace = $"CREATE TABLESPACE \"{tablespaceName!.Replace("\"", "\"\"")}\" LOCATION '{directory!.Replace("'", "''")}'";

                    await using NpgsqlCommand npgsqlCommand_CreateTablespace = new (commandText_CreateTablespace, npgsqlConnection);
                    await npgsqlCommand_CreateTablespace.ExecuteNonQueryAsync();
                }
            }

            // 2. Database existence check
            string databaseName = connectionData.Database;
            string commandText_Select = "SELECT 1 FROM pg_database WHERE datname = @databaseName";

            await using (NpgsqlCommand npgsqlCommand = new(commandText_Select, npgsqlConnection))
            {
                npgsqlCommand.Parameters.AddWithValue("databaseName", databaseName);
                if (await npgsqlCommand.ExecuteScalarAsync() != null)
                {
                    return true;
                }
            }

            // 3. Database creation
            // Build the command string based on whether we use a tablespace or not
            string commandText_Create = $"CREATE DATABASE \"{databaseName.Replace("\"", "\"\"")}\"";

            if (useTablespace)
            {
                commandText_Create += $" TABLESPACE \"{tablespaceName!.Replace("\"", "\"\"")}\"";
            }

            await using NpgsqlCommand npgsqlCommand_Create = new(commandText_Create, npgsqlConnection);
            await npgsqlCommand_Create.ExecuteNonQueryAsync();

            return true;
        }

        public static async Task<bool> DatabaseAsync(PostgreSQLConfigurationFile? postgreSQLConfigurationFile)
        {
            if(postgreSQLConfigurationFile is null)
            {
                return false;
            }

            return await DatabaseAsync(ConnectionData(postgreSQLConfigurationFile), postgreSQLConfigurationFile.Tablespace, postgreSQLConfigurationFile.Directory);
        }
    }
}