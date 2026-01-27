using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public async static Task<bool> RemoveDatabaseAsync(this ConnectionData? connectionData, string databaseName, string tablespaceName)
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
            await DropTablespaceAsync(connectionData, tablespaceName);

            return true;
        }
    }
}