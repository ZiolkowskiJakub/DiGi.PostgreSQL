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
        /// <summary>
        /// Asynchronously counts the number of records for a specified type in the database.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance used to execute the query.</param>
        /// <param name="type">The type of the objects to count.</param>
        /// <param name="inheritance">A value indicating whether to include inherited types in the count.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the total record count, or -1 if the connection or type is null.</returns>
        public static async Task<long> CountAsync(this NpgsqlConnection npgsqlConnection, Type? type, bool inheritance = true)
        {
            if (npgsqlConnection is null || type is null)
            {
                return -1;
            }

            IEnumerable<short>? partitionIds = null;
            if (!inheritance)
            {
                short? partitionId = await PartitionIdAsync(npgsqlConnection, type);
                if (partitionId is not null)
                {
                    partitionIds = [partitionId.Value];
                }
            }
            else
            {
                List<Partition>? partitions = await PartitionsAsync(npgsqlConnection, type);
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