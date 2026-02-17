using DiGi.PostgreSQL.Classes;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Query
    {
        public static async Task<long> CountAsync(this NpgsqlConnection npgsqlConnection, Type? type, bool inheritance = true)
        {
            if (npgsqlConnection is null || type is null)
            {
                return -1;
            }

            IEnumerable<short>? partitionIds = null;
            if (!inheritance)
            {
                short? partitionId = await PartitionId(npgsqlConnection, type);
                if (partitionId is not null)
                {
                    partitionIds = [partitionId.Value];
                }
            }
            else
            {
                List<Partition>? partitions = await Partitions(npgsqlConnection, type);
                if (partitions is null || partitions.Count == 0)
                {
                    return 0;
                }

                partitionIds = partitions.ConvertAll(x => x.Id);
            }

            if (partitionIds is null || !partitionIds.Any())
            {
                return 0;
            }

            return await npgsqlConnection.CountAsync(partitionIds);
        }
    }
}