using Npgsql;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves a type from the database by its name.
        /// </summary>
        /// <param name="npgsqlConnection">The connection to the PostgreSQL database.</param>
        /// <param name="name">The name of the type to retrieve.</param>
        /// <param name="commandTimeout">The timeout in seconds for the execution of the command. A value of 0 disables the timeout.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A <see cref="Classes.Type"/> instance if the type is found; otherwise, <c>null</c>.</returns>
        public static async Task<Classes.Type?> TypeAsync(this NpgsqlConnection? npgsqlConnection, string? name, int commandTimeout = 30, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
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
                return new Classes.Type(npgsqlDataReader.GetInt16(0), name);
            }

            return null;
        }
    }
}