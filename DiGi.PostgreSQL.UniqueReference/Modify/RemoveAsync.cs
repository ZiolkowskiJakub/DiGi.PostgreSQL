using DiGi.Core.Interfaces;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Modify
    {
        public static async Task<List<TUniqueReference>?> RemoveAsync<TUniqueReference>(NpgsqlConnection? npgsqlConnection, IEnumerable<TUniqueReference> uniqueReferences) where TUniqueReference : IUniqueReference
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

            List<TUniqueReference> result = [];
            HashSet<short> partitionIds = [];

            await using NpgsqlCommand npgsqlCommand = new(commandText_Delete, npgsqlConnection);
            npgsqlCommand.Parameters.Add("name", NpgsqlDbType.Text);
            npgsqlCommand.Parameters.Add("unique_id", NpgsqlDbType.Text);

            foreach (TUniqueReference uniqueReference in uniqueReferences)
            {
                if (uniqueReference?.TypeReference?.FullTypeName is not string fullTypeName || string.IsNullOrWhiteSpace(uniqueReference.UniqueId))
                {
                    continue;
                }

                npgsqlCommand.Parameters["name"].Value = fullTypeName;
                npgsqlCommand.Parameters["unique_id"].Value = uniqueReference.UniqueId;

                if (await npgsqlCommand.ExecuteScalarAsync() is short partitionId)
                {
                    result.Add(uniqueReference);
                    partitionIds.Add(partitionId); // Track this type for cleanup
                }
            }

            await PostgreSQL.Modify.CleanPartitions(npgsqlConnection, partitionIds);

            return result;
        }

        public static async Task<bool> RemoveAsync(NpgsqlConnection? npgsqlConnection, Type type, bool inheritance = true)
        {
            if (npgsqlConnection is null || type is null)
            {
                return false;
            }

            IEnumerable<short>? partitionIds = null;
            if (!inheritance)
            {
                short? partitionId = await Query.PartitionId(npgsqlConnection, type);
                if (partitionId is not null)
                {
                    partitionIds = [partitionId.Value];
                }
            }
            else
            {
                Dictionary<short, Type>? dictionary = await Query.PartitionIds(npgsqlConnection, type);
                if (dictionary is null || dictionary.Count == 0)
                {
                    return false;
                }
                partitionIds = [.. dictionary.Keys];
            }

            if (partitionIds is null || !partitionIds.Any())
            {
                return false;
            }

            return await PostgreSQL.Modify.RemoveAsync(npgsqlConnection, partitionIds);
        }
    }
}