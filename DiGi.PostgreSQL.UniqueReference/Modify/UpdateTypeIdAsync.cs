using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Modify
    {
        public static async Task<short?> UpdateTypeIdAsync(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null || Core.Query.FullTypeName(type) is not string fullName)
            {
                return null;
            }

            return await PostgreSQL.Modify.UpdateTypeIdAsync(npgsqlConnection, fullName);
        }
    }
}