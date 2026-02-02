using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<bool> RemoveAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<short>? partitionIds)
        {
            if (npgsqlConnection is null || partitionIds == null || !partitionIds.Any())
            {
                return false;
            }

            try
            {
                await using NpgsqlCommand npgsqlCommand = new("DELETE FROM objects WHERE partition_id = ANY(@partition_ids);", npgsqlConnection);

                // Przekazujemy całą tablicę jako jeden parametr
                npgsqlCommand.Parameters.AddWithValue("partition_ids", partitionIds);

                int count = await npgsqlCommand.ExecuteNonQueryAsync();
                if (count > 0)
                {
                    await CleanPartitions(npgsqlConnection, partitionIds);
                }

                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}