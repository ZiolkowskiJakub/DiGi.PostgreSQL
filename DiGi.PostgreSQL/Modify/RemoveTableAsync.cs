using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
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