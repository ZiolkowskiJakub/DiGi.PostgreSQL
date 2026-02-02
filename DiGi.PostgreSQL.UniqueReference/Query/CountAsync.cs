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

            IEnumerable<short>? typeIds = null;
            if (!inheritance)
            {
                short? typeId = await PartitionId(npgsqlConnection, type);
                if (typeId is not null)
                {
                    typeIds = [typeId.Value];
                }
            }
            else
            {
                Dictionary<short, Type>? dictionary = await PartitionIds(npgsqlConnection, type);
                if (dictionary is null || dictionary.Count == 0)
                {
                    return 0;
                }
                typeIds = [.. dictionary.Keys];
            }

            if (typeIds is null || !typeIds.Any())
            {
                return 0;
            }

            return await npgsqlConnection.CountAsync(typeIds);
        }
    }
}