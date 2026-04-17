using Npgsql;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<long> EstimatedCountAsync(this NpgsqlConnection npgsqlConnection, string tableName, bool analyze = false, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName))
            {
                return -1;
            }

            if(analyze)
            {
                // Explicitly run ANALYZE to refresh statistics
                string commandText_Analyze = $"ANALYZE {tableName}";
                using NpgsqlCommand npgsqlCommand_Analyze = new(commandText_Analyze, npgsqlConnection);

                await npgsqlCommand_Analyze.ExecuteNonQueryAsync();
            }

            // Querying the system catalogs for an estimate
            const string commandText_Select = "SELECT reltuples AS estimate FROM pg_class WHERE relname = @tableName";

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