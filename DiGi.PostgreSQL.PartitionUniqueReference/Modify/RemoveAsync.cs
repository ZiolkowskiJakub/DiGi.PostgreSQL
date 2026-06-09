using DiGi.Core;
using DiGi.PostgreSQL.Classes;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Modify
    {
        /// <summary>
        /// Removes the specified partition unique references from the database asynchronously.
        /// </summary>
        /// <param name="npgsqlConnection">The connection to the PostgreSQL database.</param>
        /// <param name="partitionUniqueReferences">The collection of partition unique references to be removed.</param>
        /// <param name="clean">A value indicating whether to perform cleanup of partitions and types after removal.</param>
        /// <returns>A HashSet of PartitionUniqueReference containing the successfully removed references, or <c>null</c> if the operation failed or no references were processed.</returns>
        public static async Task<HashSet<Classes.PartitionUniqueReference>?> RemoveAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<Classes.PartitionUniqueReference> partitionUniqueReferences, bool clean = true)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            Dictionary<string, List<Classes.PartitionUniqueReference>>? dictionary = Core.Convert.ToSystem_Dictionary(partitionUniqueReferences, x => x?.Name);
            if (dictionary is null || dictionary.Count == 0)
            {
                return null;
            }

            List<Partition>? partitions = await PostgreSQL.Query.PartitionsAsync(npgsqlConnection, dictionary.Keys);
            if (partitions is null || partitions.Count == 0)
            {
                return null;
            }

            HashSet<Classes.PartitionUniqueReference> result = [];

            foreach (KeyValuePair<string, List<Classes.PartitionUniqueReference>> keyValuePair in dictionary)
            {
                Partition? partition = partitions.Find(x => x.Name == keyValuePair.Key);
                if (partition is null)
                {
                    continue;
                }

                List<Classes.PartitionUniqueReference>? partitionUniqueReferences_Temp = keyValuePair.Value;

                while (partitionUniqueReferences_Temp != null && partitionUniqueReferences_Temp.Count != 0)
                {
                    string? fullTypeName = partitionUniqueReferences_Temp[0].UniqueReference?.TypeReference?.FullTypeName;

                    partitionUniqueReferences_Temp.Filter(x => x?.UniqueReference?.TypeReference?.FullTypeName == fullTypeName, out List<Classes.PartitionUniqueReference>? partitionUniqueReferences_In, out List<Classes.PartitionUniqueReference>? partitionUniqueReferences_Out);
                    if (partitionUniqueReferences_In is null || partitionUniqueReferences_In.Count == 0)
                    {
                        break;
                    }

                    partitionUniqueReferences_Temp = partitionUniqueReferences_Out;

                    if (string.IsNullOrWhiteSpace(fullTypeName))
                    {
                        continue;
                    }

                    Classes.Type? type = await Query.TypeAsync(npgsqlConnection, fullTypeName);
                    if (type is null)
                    {
                        continue;
                    }

                    string commandText = $@"
                        DELETE FROM objects_{(int)partition.DataType} o
                        WHERE o.partition_id = @partition_id
                          AND o.unique_id = @unique_id
                          AND o.type_id = @type_id
                        RETURNING o.partition_id;";

                    await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                    npgsqlCommand.Parameters.Add("partition_id", NpgsqlDbType.Smallint);
                    npgsqlCommand.Parameters.Add("type_id", NpgsqlDbType.Smallint);
                    npgsqlCommand.Parameters.Add("unique_id", NpgsqlDbType.Text);

                    foreach (Classes.PartitionUniqueReference partitionUniqueReference in partitionUniqueReferences_In)
                    {
                        if (partitionUniqueReference?.UniqueReference?.TypeReference?.FullTypeName is not string fullTypeName_Temp || partitionUniqueReference?.UniqueReference?.UniqueId is not string uniqueId)
                        {
                            continue;
                        }

                        npgsqlCommand.Parameters["partition_id"].Value = partition.Id;
                        npgsqlCommand.Parameters["type_id"].Value = type.Id;
                        npgsqlCommand.Parameters["unique_id"].Value = uniqueId;

                        if (await npgsqlCommand.ExecuteScalarAsync() is short partitionId)
                        {
                            result.Add(partitionUniqueReference);
                        }
                    }
                }
            }

            if (clean)
            {
                await PostgreSQL.Modify.CleanPartitionsAsync(npgsqlConnection);
                await CleanTypesAsync(npgsqlConnection);
            }

            return result;
        }
    }
}