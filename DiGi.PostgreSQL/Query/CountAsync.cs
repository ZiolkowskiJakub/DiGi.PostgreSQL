using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<long> CountAsync(this NpgsqlConnection npgsqlConnection, IEnumerable<short> partitionIds)
        {
            if (npgsqlConnection is null || partitionIds is null)
            {
                return -1;
            }

            if (!partitionIds.Any())
            {
                return 0;
            }

            // Summing up everything that matches any ID in the provided array
            const string commandText = "SELECT COUNT(*) FROM objects WHERE partition_id = ANY(@partition_ids)";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("partition_ids", partitionIds);

            var result = await npgsqlCommand.ExecuteScalarAsync();

            // If no rows match, PostgreSQL returns 0; ExecuteScalar returns long for COUNT
            return result is long count ? count : 0;
        }
    }
}