using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Modify
    {
        public static async Task<short?> UpdateTypeIdAsync(this NpgsqlConnection? npgsqlConnection, System.Type? type, Enums.DataType dataType)
        {
            if (npgsqlConnection is null || type is null || Core.Query.FullTypeName(type) is not string fullName)
            {
                return null;
            }

            Partition? partition = await PostgreSQL.Modify.UpdatePartitionIdAsync(npgsqlConnection, fullName, dataType);
            if (partition is null)
            {
                return null;
            }

            return partition.Id;
        }
    }
}