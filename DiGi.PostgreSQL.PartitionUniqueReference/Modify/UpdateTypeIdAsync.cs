using Npgsql;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Modify
    {
        /// <summary>
        /// Updates or creates a type ID based on the provided name.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance used to perform the operation.</param>
        /// <param name="name">The name of the type to update or create.</param>
        /// <param name="commandTimeout">The timeout in seconds for the execution of the command. A value of 0 disables the timeout.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous operation, containing the updated or created <see cref="Classes.Type"/> object, or null if the operation failed or inputs were invalid.</returns>
        public static async Task<Classes.Type?> UpdateTypeIdAsync(this NpgsqlConnection? npgsqlConnection, string? name, int commandTimeout = 30, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            Classes.Type? result = await Query.TypeAsync(npgsqlConnection, name, commandTimeout: commandTimeout, cancellationToken: cancellationToken);
            if (result is null)
            {
                string commandText = @"
                INSERT INTO types (name)
                VALUES (@name)
                RETURNING id;";

                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                npgsqlCommand.CommandTimeout = commandTimeout;
                npgsqlCommand.Parameters.AddWithValue("name", name);

                short? typeId = (short?)await npgsqlCommand.ExecuteScalarAsync(cancellationToken);
                if (typeId is null)
                {
                    return null;
                }

                result = await Query.TypeAsync(npgsqlConnection, name, commandTimeout: commandTimeout, cancellationToken: cancellationToken);
            }

            return result;
        }
    }
}