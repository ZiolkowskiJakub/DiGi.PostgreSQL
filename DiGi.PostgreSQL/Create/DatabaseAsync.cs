using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;
using System.IO;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        // Improved version with parameterization
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
            await using (NpgsqlCommand npgsqlCommand_Create = new NpgsqlCommand(commandText_Create, npgsqlConnection))
            {
                await npgsqlCommand_Create.ExecuteNonQueryAsync();
            }

            return true;
        }

        public static async Task<bool> DatabaseAsync(this ConnectionData? connectionData, string tablespaceName, string directory)
        {
            if (connectionData is null || string.IsNullOrWhiteSpace(connectionData.Database))
            {
                return false;
            }

            if (!Directory.Exists(directory))
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

            // 1. Tablespace existence check
            bool tablespaceExists = false;

            string commandText_Select = "SELECT 1 FROM pg_tablespace WHERE spcname = @tablespaceName";
            await using (NpgsqlCommand npgsqlCommand_Select = new (commandText_Select, npgsqlConnection))
            {
                npgsqlCommand_Select.Parameters.AddWithValue("tablespaceName", tablespaceName);
                tablespaceExists = (await npgsqlCommand_Select.ExecuteScalarAsync()) != null;
            }

            string commandText_Create;

            if (!tablespaceExists)
            {
                // Directory must be an absolute path and PostgreSQL user must have permissions
                commandText_Create = $"CREATE TABLESPACE \"{tablespaceName.Replace("\"", "\"\"")}\" LOCATION @directory";
                await using NpgsqlCommand npgsqlCommand_Create = new(commandText_Create, npgsqlConnection);
                npgsqlCommand_Create.Parameters.AddWithValue("directory", directory);
                await npgsqlCommand_Create.ExecuteNonQueryAsync();
            }

            // 2. Database creation with Tablespace
            // Logic similar to above, ensuring the database is linked to the tablespace
            //return await ExecuteDatabaseCreationWithTablespace(npgsqlConnection, connectionData.Database, tablespaceName);

            string databaseName = connectionData.Database;

            commandText_Select = "SELECT 1 FROM pg_database WHERE datname = @databaseName";
            await using (NpgsqlCommand npgsqlCommand = new(commandText_Select, npgsqlConnection))
            {
                npgsqlCommand.Parameters.AddWithValue("databaseName", databaseName);
                if (await npgsqlCommand.ExecuteScalarAsync() != null)
                {
                    return true;
                }
            }

            commandText_Create = $"CREATE DATABASE \"{databaseName.Replace("\"", "\"\"")}\" TABLESPACE \"{tablespaceName.Replace("\"", "\"\"")}\"";
            await using NpgsqlCommand NpgsqlCommand_Create = new(commandText_Create, npgsqlConnection);

            await NpgsqlCommand_Create.ExecuteNonQueryAsync();

            return true;
        }
    }
}