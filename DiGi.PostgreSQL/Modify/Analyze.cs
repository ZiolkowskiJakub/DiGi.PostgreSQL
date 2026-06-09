using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        /// <summary>
        /// Performs an ANALYZE operation on the specified table to update statistics for the query planner.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection used to execute the command.</param>
        /// <param name="tableName">The name of the table to be analyzed.</param>
        /// <param name="commandTimeout">The timeout in seconds for the execution of the command.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the analysis was successful; otherwise, false.</returns>
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