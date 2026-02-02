using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<List<short>?> CleanPartitions(NpgsqlConnection? npgsqlConnection, IEnumerable<short>? partitionIds = null)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            partitionIds ??= (await Query.PartitionIds(npgsqlConnection))?.Keys;

            if (partitionIds is null)
            {
                return null;
            }

            List<short> result = [];

            if (!partitionIds.Any())
            {
                return result;
            }

            foreach (short partitionId in partitionIds)
            {
                // Check if the partition is now empty
                await using NpgsqlCommand npgsqlCommand_Check = new("SELECT NOT EXISTS(SELECT 1 FROM objects WHERE partition_id = @partition_id);", npgsqlConnection);
                npgsqlCommand_Check.Parameters.Add("partition_id", NpgsqlDbType.Smallint).Value = partitionId;

                bool isEmpty = (bool)(await npgsqlCommand_Check.ExecuteScalarAsync() ?? false);
                if (isEmpty)
                {
                    // 1. Remove from types first (Metadata)
                    await using NpgsqlCommand npgsqlCommand_DeleteType = new("DELETE FROM partitions WHERE id = @partition_id;", npgsqlConnection);
                    npgsqlCommand_DeleteType.Parameters.Add("partition_id", NpgsqlDbType.Smallint).Value = partitionId;
                    await npgsqlCommand_DeleteType.ExecuteNonQueryAsync();

                    // 2. Drop the physical partition table
                    await using NpgsqlCommand npgsqlCommand_DropTable = new($"DROP TABLE IF EXISTS objects_{partitionId};", npgsqlConnection);
                    await npgsqlCommand_DropTable.ExecuteNonQueryAsync();

                    result.Add(partitionId);
                }
            }

            return result;
        }
    }
}