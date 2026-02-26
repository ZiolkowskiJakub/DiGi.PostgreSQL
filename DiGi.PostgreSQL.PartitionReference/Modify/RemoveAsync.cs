using DiGi.PostgreSQL.Classes;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionReference
{
    public static partial class Modify
    {
        public static async Task<HashSet<Classes.PartitionReference>?> RemoveAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<Classes.PartitionReference> partitionReferences)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            Dictionary<string, List<Classes.PartitionReference>>? dictionary = Core.Convert.ToSystem_Dictionary(partitionReferences, x => x?.Name);
            if (dictionary is null)
            {
                return null;
            }

            HashSet<Classes.PartitionReference> result = [];
            HashSet<short> partitionIds = [];

            foreach (KeyValuePair<string, List<Classes.PartitionReference>> keyValuePair in dictionary)
            {
                Partition? partition = await PostgreSQL.Query.PartitionAsync(npgsqlConnection, keyValuePair.Key);
                if (partition is null)
                {
                    continue;
                }

                string commandText_Delete = $@"
                DELETE FROM objects_{(int)partition.DataType} o
                USING partitions t
                WHERE o.partition_id = t.id
                  AND t.name = @name
                  AND o.unique_id = @unique_id
                RETURNING o.partition_id;";

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
            }

            await PostgreSQL.Modify.CleanPartitionsAsync(npgsqlConnection, partitionIds);

            return result;
        }
    }
}