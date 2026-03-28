using DiGi.PostgreSQL.Classes;
using Npgsql;
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
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            // Check if database exists using parameters to prevent SQL Injection
            string commandText_Select = "SELECT 1 FROM pg_database WHERE datname = @databaseName";
            await using (NpgsqlCommand npgsqlCommand_Select = new(commandText_Select, npgsqlConnection))
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

            // 1. Check if the database exists anywhere in the cluster first
            // We connect to the maintenance database to perform discovery
            ConnectionData connectionData_Temp = connectionData.GetDefault();
            string targetDatabaseName = connectionData.Database;

            await using NpgsqlConnection? npgsqlConnection = NpgsqlConnection(connectionData_Temp);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            string checkDbSql = "SELECT 1 FROM pg_database WHERE datname = @dbName";
            bool databaseExists = false;

            await using (NpgsqlCommand npgsqlCommand_Check = new NpgsqlCommand(checkDbSql, npgsqlConnection))
            {
                npgsqlCommand_Check.Parameters.AddWithValue("dbName", targetDatabaseName);
                object? result = await npgsqlCommand_Check.ExecuteScalarAsync();
                databaseExists = result != null;
            }

            // If the database already exists, we exit early.
            // We don't want to attempt creating tablespaces or re-creating the DB.
            if (databaseExists)
            {
                return true;
            }

            // 2. Handle Tablespace logic only if the database does NOT exist
            bool useTablespace = !string.IsNullOrWhiteSpace(tablespaceName) && !string.IsNullOrWhiteSpace(directory);

            if (useTablespace)
            {
                // Ensure the physical directory exists on the server's filesystem
                if (!Directory.Exists(directory))
                {
                    return false;
                }

                string checkTsSql = "SELECT 1 FROM pg_tablespace WHERE spcname = @tsName";
                bool tablespaceExists = false;

                await using (NpgsqlCommand npgsqlCommand_TsCheck = new NpgsqlCommand(checkTsSql, npgsqlConnection))
                {
                    npgsqlCommand_TsCheck.Parameters.AddWithValue("tsName", tablespaceName!);
                    object? tsResult = await npgsqlCommand_TsCheck.ExecuteScalarAsync();
                    tablespaceExists = tsResult != null;
                }

                if (!tablespaceExists)
                {
                    string escapedTsName = tablespaceName!.Replace("\"", "\"\"");
                    string escapedDir = directory!.Replace("'", "''");
                    string createTsSql = $"CREATE TABLESPACE \"{escapedTsName}\" LOCATION '{escapedDir}'";

                    await using NpgsqlCommand npgsqlCommand_CreateTs = new NpgsqlCommand(createTsSql, npgsqlConnection);
                    await npgsqlCommand_CreateTs.ExecuteNonQueryAsync();
                }
            }

            // 3. Create the Database
            string escapedDbName = targetDatabaseName.Replace("\"", "\"\"");
            string createDbSql = $"CREATE DATABASE \"{escapedDbName}\"";

            if (useTablespace)
            {
                string escapedTsName = tablespaceName!.Replace("\"", "\"\"");
                createDbSql += $" TABLESPACE \"{escapedTsName}\"";
            }

            try
            {
                await using NpgsqlCommand npgsqlCommand_Create = new NpgsqlCommand(createDbSql, npgsqlConnection);
                await npgsqlCommand_Create.ExecuteNonQueryAsync();
                return true;
            }
            catch (NpgsqlException)
            {
                // Handle potential race conditions or permission issues
                return false;
            }
        }

        public static async Task<bool> DatabaseAsync(PostgreSQLConfigurationFile? postgreSQLConfigurationFile)
        {
            if (postgreSQLConfigurationFile is null)
            {
                return false;
            }

            return await DatabaseAsync(ConnectionData(postgreSQLConfigurationFile), postgreSQLConfigurationFile.Tablespace, postgreSQLConfigurationFile.Directory);
        }
    }
}