using DiGi.PostgreSQL.Classes;
using Npgsql;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Checks if a table exists in the PostgreSQL database using the provided connection.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the query.</param>
        /// <param name="tableName">The name of the table to check for existence.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains true if the table exists; otherwise, false.</returns>
        public static async Task<bool> TableExistsAsync(this NpgsqlConnection? npgsqlConnection, string tableName, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            // Explicitly cast to text so Npgsql can handle the return value
            using NpgsqlCommand npgsqlCommand = new("SELECT to_regclass(@tableName)::text;", npgsqlConnection);

            // It's safer to use the parameter name without @ in AddWithValue,
            // though Npgsql handles both.
            npgsqlCommand.Parameters.AddWithValue("tableName", $"public.{tableName}");

            object? result = await npgsqlCommand.ExecuteScalarAsync(cancellationToken);

            // If the table doesn't exist, to_regclass returns NULL (DBNull.Value in C#)
            return result != null && result != DBNull.Value;
        }

        /// <summary>
        /// Checks if a table exists in the PostgreSQL database using the provided connection data.
        /// </summary>
        /// <param name="connectionData">The connection data used to create the Npgsql connection.</param>
        /// <param name="tableName">The name of the table to check for existence.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains true if the table exists; otherwise, false.</returns>
        public static async Task<bool> TableExistsAsync(this ConnectionData? connectionData, string tableName, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            if (Create.NpgsqlConnection(connectionData) is not NpgsqlConnection npgsqlConnection)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync(cancellationToken);

            return await TableExistsAsync(npgsqlConnection, tableName, cancellationToken: cancellationToken);
        }
    }
}