using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Query
    {
        public static async Task<short?> PartitionId(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null || Core.Query.FullTypeName(type) is not string fullName)
            {
                return null;
            }

            return await PostgreSQL.Query.PartitionId(npgsqlConnection, fullName);
        }
    }
}