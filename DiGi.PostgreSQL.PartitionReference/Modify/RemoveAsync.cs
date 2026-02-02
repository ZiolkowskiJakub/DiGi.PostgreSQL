using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionReference
{
    public static partial class Modify
    {
        public static async Task<List<Classes.PartitionReference>?> RemoveAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<Classes.PartitionReference> partitionReferences)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            // Use RETURNING o.type_id so we know exactly which partition was affected
            string commandText_Delete = @"
                DELETE FROM objects o
                USING partitions t
                WHERE o.partition_id = t.id
                  AND t.name = @name
                  AND o.unique_id = @unique_id
                RETURNING o.partition_id;";

            List<Classes.PartitionReference> result = [];
            HashSet<short> partitionIds = [];

            await using NpgsqlCommand npgsqlCommand = new(commandText_Delete, npgsqlConnection);
            npgsqlCommand.Parameters.Add("name", NpgsqlDbType.Text);
            npgsqlCommand.Parameters.Add("unique_id", NpgsqlDbType.Text);

            foreach (Classes.PartitionReference partitionReference in partitionReferences)
            {
                if (partitionReference?.Name is not string name || string.IsNullOrWhiteSpace(partitionReference.UniqueId))
                {
                    continue;
                }

                npgsqlCommand.Parameters["name"].Value = name;
                npgsqlCommand.Parameters["unique_id"].Value = partitionReference.UniqueId;

                if (await npgsqlCommand.ExecuteScalarAsync() is short partitionId)
                {
                    result.Add(partitionReference);
                    partitionIds.Add(partitionId); // Track this type for cleanup
                }
            }

            await PostgreSQL.Modify.CleanPartitions(npgsqlConnection, partitionIds);

            return result;
        }
    }
}