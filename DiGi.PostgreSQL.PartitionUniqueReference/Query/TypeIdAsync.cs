using Npgsql;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves the unique identifier of a type from the database based on its full name.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection used to execute the query.</param>
        /// <param name="type">The system type for which the ID is being retrieved.</param>
        /// <param name="commandTimeout">The timeout in seconds for the execution of the command. A value of 0 disables the timeout.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the type identifier as a short if found; otherwise, null.</returns>
        public static async Task<short?> TypeIdAsync(this NpgsqlConnection? npgsqlConnection, System.Type? type, int commandTimeout = 30, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || type is null || Core.Query.FullTypeName(type) is not string name)
            {
                return null;
            }

            string commandText = @"
                SELECT id
                FROM types
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