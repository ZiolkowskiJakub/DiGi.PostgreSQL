using Npgsql;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves the partition ID associated with the specified name.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection used to execute the query.</param>
        /// <param name="name">The name of the partition to retrieve the ID for.</param>
        /// <param name="commandTimeout">The timeout in seconds for the execution of the command. A value of 0 disables the timeout.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the partition ID as a short if found; otherwise, null.</returns>
        public static async Task<short?> PartitionIdAsync(this NpgsqlConnection? npgsqlConnection, string? name, int commandTimeout = 30, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            string commandText = @"
                SELECT id
                FROM partitions
                WHERE name = @name;
                ";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            npgsqlCommand.CommandTimeout = commandTimeout;

            npgsqlCommand.Parameters.AddWithValue("name", name);

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync(cancellationToken);
            if (await npgsqlDataReader.ReadAsync(cancellationToken))
            {
                return npgsqlDataReader.GetInt16(0);
            }

            return null;
        }
    }
}