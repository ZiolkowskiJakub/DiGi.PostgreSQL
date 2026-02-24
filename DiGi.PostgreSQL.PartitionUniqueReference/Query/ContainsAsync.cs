using Npgsql;
using System;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Query
    {
        public static async Task<bool> ContainsAsync(this NpgsqlConnection npgsqlConnection, Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return false;
            }

            return await PostgreSQL.Query.ContainsAsync(npgsqlConnection, Core.Query.FullTypeName(type));
        }
    }
}