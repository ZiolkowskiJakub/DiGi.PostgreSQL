using Npgsql;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Gets an estimated row count for the specified table in a PostgreSQL database.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the query.</param>
        /// <param name="tableName">The name of the table to get the estimate for.</param>
        /// <param name="analyze">A boolean indicating whether to run VACUUM ANALYZE before fetching the count.</param>
        /// <param name="commandTimeout">The timeout in seconds applied to every command executed. A value of 0 disables the timeout.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>The estimated number of rows as a nullable long, -1 if the table exists but has not been analysed, or null if the table does not exist or connection is invalid.</returns>
        public static async Task<long?> EstimatedCountAsync(this NpgsqlConnection? npgsqlConnection, string tableName, bool analyze = false, int commandTimeout = 600, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName))
            {
                return null;
            }

            if (!await TableExistsAsync(npgsqlConnection, tableName, cancellationToken: cancellationToken))
            {
                return null;
            }

            if (analyze)
            {
                // Explicitly run ANALYZE to refresh statistics. The existence check above is what makes
                // the name safe to place in the statement; it is quoted rather than pasted in raw.
                string commandText_Analyze = $"VACUUM ANALYZE \"{tableName}\"";
                await using NpgsqlCommand npgsqlCommand_Analyze = new(commandText_Analyze, npgsqlConnection);
                npgsqlCommand_Analyze.CommandTimeout = commandTimeout;

                await npgsqlCommand_Analyze.ExecuteNonQueryAsync(cancellationToken);
            }

            // Querying the system catalogs for an estimate
            const string commandText_Select = "SELECT reltuples AS estimate FROM pg_class WHERE oid = to_regclass(@tableName)";

            await using NpgsqlCommand npgsqlCommand = new(commandText_Select, npgsqlConnection);
            npgsqlCommand.CommandTimeout = commandTimeout;

            npgsqlCommand.Parameters.AddWithValue("tableName", tableName);
            object? @object = await npgsqlCommand.ExecuteScalarAsync(cancellationToken);
            if (@object is long @long)
            {
                return @long;
            }
            else if (@object is int @int)
            {
                return @int;
            }
            else if (@object is float @float)
            {
                return (long)@float;
            }
            else if (@object is double @double)
            {
                return (long)@double;
            }
            else if (Core.Query.IsNumeric(@object))
            {
                return System.Convert.ToInt64(@object);
            }

            return -1;
        }
    }
}