using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<bool> Analyze(NpgsqlConnection? npgsqlConnection, string? tableName, int commandTimeout = 30)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            // Run ANALYZE to populate statistics for the query planner.
            // This ensures that the GiST index and hierarchy index are utilized from the very first query.
            string commandText = $"ANALYZE {tableName};";

            try
            {
                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                npgsqlCommand.CommandTimeout = commandTimeout;

                await npgsqlCommand.ExecuteNonQueryAsync();
            }
            catch (NpgsqlException)
            {
                return false;
            }

            return true;
        }
    }
}