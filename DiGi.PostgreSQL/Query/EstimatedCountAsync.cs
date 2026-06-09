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
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>The estimated number of rows as a long, or -1 if an error occurs or the table does not exist.</returns>
        public static async Task<long> EstimatedCountAsync(this NpgsqlConnection npgsqlConnection, string tableName, bool analyze = false, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName))
            {
                return -1;
            }

            if (!await TableExistsAsync(npgsqlConnection, tableName))
            {
                return -1;
            }

            if (analyze)
            {
                // Explicitly run ANALYZE to refresh statistics
                string commandText_Analyze = $"VACUUM ANALYZE {tableName}";
                using NpgsqlCommand npgsqlCommand_Analyze = new(commandText_Analyze, npgsqlConnection);

                await npgsqlCommand_Analyze.ExecuteNonQueryAsync();
            }

            // Querying the system catalogs for an estimate
            const string commandText_Select = "SELECT reltuples AS estimate FROM pg_class WHERE oid = @tableName::regclass"; ;

            using NpgsqlCommand npgsqlCommand = new(commandText_Select, npgsqlConnection);

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
            else if (Core.Query.IsNumeric(@object))
            {
                return System.Convert.ToInt64(@object);
            }

            return -1;
        }
    }
}