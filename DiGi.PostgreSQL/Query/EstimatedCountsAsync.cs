using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Gets estimated row counts for many tables at once, reading the planner's statistics for the whole set in a single catalog query per batch.
        /// <para>This is the plural form of <see cref="EstimatedCountAsync(NpgsqlConnection?, string, bool, int, CancellationToken)"/> and exists because calling the singular in a loop issues two round trips per table - one existence check and one catalog read. Reading <c>pg_class</c> by name answers both questions at once: a table that does not exist simply produces no row.</para>
        /// <para>A table is absent from the result when it does not exist, and carries <c>-1</c> when it exists but has never been analysed, mirroring the <c>null</c> and <c>-1</c> the singular returns for those two cases.</para>
        /// <para>Setting <paramref name="analyze"/> costs one <c>VACUUM ANALYZE</c> statement per existing table. That work is per table by construction and cannot be batched, so the cost grows with the size of <paramref name="tableNames"/> - budget <paramref name="commandTimeout"/> accordingly.</para>
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the query.</param>
        /// <param name="tableNames">The names of the tables to estimate. Blank entries and duplicates are ignored.</param>
        /// <param name="analyze">A boolean indicating whether to run VACUUM ANALYZE on each existing table before reading the estimates.</param>
        /// <param name="batchSize">The maximum number of table names sent in a single catalog query.</param>
        /// <param name="commandTimeout">The timeout in seconds applied to every command executed. A value of 0 disables the timeout.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A dictionary keyed by table name holding the estimated row count for every table that exists, an empty dictionary when no usable name was supplied, or null when the connection or the names are null.</returns>
        public static async Task<Dictionary<string, long>?> EstimatedCountsAsync(this NpgsqlConnection? npgsqlConnection, IEnumerable<string>? tableNames, bool analyze = false, int batchSize = 1000, int commandTimeout = 600, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || tableNames is null)
            {
                return null;
            }

            List<string> tableNames_Temp = [.. tableNames.Where(x => !string.IsNullOrWhiteSpace(x)).Distinct()];

            Dictionary<string, long> result = [];

            if (tableNames_Temp.Count == 0)
            {
                return result;
            }

            // reltuples is cast in SQL so the reader gets one predictable type rather than a float4 that has to be widened here.
            const string commandText = @"
                SELECT c.relname, c.reltuples::bigint AS estimate
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relkind IN ('r', 'p')
                  AND c.relname = ANY(@tableNames);";

            async Task ReadEstimatesAsync(List<string> tableNames_Read)
            {
                for (int i = 0; i < tableNames_Read.Count; i += batchSize)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    string[] tableNames_Chunk = [.. tableNames_Read.Skip(i).Take(batchSize)];

                    await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                    npgsqlCommand.CommandTimeout = commandTimeout;
                    npgsqlCommand.Parameters.Add(new NpgsqlParameter("tableNames", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = tableNames_Chunk });

                    await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync(cancellationToken);
                    while (await npgsqlDataReader.ReadAsync(cancellationToken))
                    {
                        result[npgsqlDataReader.GetString(0)] = npgsqlDataReader.GetInt64(1);
                    }
                }
            }

            await ReadEstimatesAsync(tableNames_Temp);

            if (!analyze || result.Count == 0)
            {
                return result;
            }

            // The catalog read above is the identifier whitelist: only relations PostgreSQL has just
            // reported by name are vacuumed, and the name is quoted rather than pasted in raw.
            List<string> tableNames_Existing = [.. result.Keys];

            foreach (string tableName in tableNames_Existing)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await using NpgsqlCommand npgsqlCommand_Analyze = new($"VACUUM ANALYZE \"{tableName}\"", npgsqlConnection);
                npgsqlCommand_Analyze.CommandTimeout = commandTimeout;

                await npgsqlCommand_Analyze.ExecuteNonQueryAsync(cancellationToken);
            }

            await ReadEstimatesAsync(tableNames_Existing);

            return result;
        }
    }
}
