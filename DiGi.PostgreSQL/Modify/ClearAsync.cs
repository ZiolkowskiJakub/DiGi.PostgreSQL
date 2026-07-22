using Npgsql;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        /// <summary>
        /// Asynchronously clears all data from the specified table and restarts its identity sequence.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to be used for the operation.</param>
        /// <param name="tableName">The name of the database table to clear.</param>
        /// <param name="commandTimeout">The timeout in seconds for the execution of the command. A value of 0 disables the timeout.</param>
        /// <param name="cancellationToken">The cancellation token to observe while waiting for the task to complete.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the operation succeeded; otherwise, false.</returns>
        public static async Task<bool> ClearAsync(NpgsqlConnection? npgsqlConnection, string tableName, int commandTimeout = 30, CancellationToken cancellationToken = default)
        {
            // Use TRUNCATE for speed, or DELETE for transactional safety
            string commandText = $"TRUNCATE TABLE {tableName} RESTART IDENTITY CASCADE;";

            using NpgsqlCommand command = new(commandText, npgsqlConnection);
            command.CommandTimeout = commandTimeout;
            try
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
                return true;
            }
            catch
            {
                // Handle logging here
                return false;
            }
        }
    }
}