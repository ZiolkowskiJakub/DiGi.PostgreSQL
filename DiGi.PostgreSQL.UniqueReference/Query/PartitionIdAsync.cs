using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves the partition identifier for a given type using the provided PostgreSQL connection.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the query.</param>
        /// <param name="type">The type for which the partition ID is being retrieved.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the partition identifier as a short, or null if not found or inputs are invalid.</returns>
        public static async Task<short?> PartitionIdAsync(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null || Core.Query.FullTypeName(type) is not string fullName)
            {
                return null;
            }

            return await PostgreSQL.Query.PartitionIdAsync(npgsqlConnection, fullName);
        }
    }
}