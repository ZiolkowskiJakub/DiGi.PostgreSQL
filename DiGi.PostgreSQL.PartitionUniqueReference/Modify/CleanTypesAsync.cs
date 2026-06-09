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
        /// Cleans up unused types from the database by removing those that are not associated with any partition.
        /// </summary>
        /// <param name="npgsqlConnection">The connection to the PostgreSQL database.</param>
        /// <returns>A list of the types that were deleted, or null if the operation could not be performed.</returns>
        public static async Task<List<Classes.Type>?> CleanTypesAsync(NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            List<Classes.Type>? types = await Query.TypesAsync(npgsqlConnection);
            if (types is null || types.Count == 0)
            {
                return null;
            }

            List<Partition>? partitions = await PostgreSQL.Query.PartitionsAsync(npgsqlConnection);
            if (partitions is null || partitions.Count == 0)
            {
                return null;
            }

            foreach (Partition partition in partitions)
            {
                HashSet<short>? typeIds = await Query.UniqueTypeIdsAsync(npgsqlConnection, partition);
                if (typeIds is null || typeIds.Count == 0)
                {
                    continue;
                }

                foreach (short typeId in typeIds)
                {
                    int index = types.FindIndex(x => x.Id == typeId);
                    if (index != -1)
                    {
                        types.RemoveAt(index);
                    }
                }
            }

            List<Classes.Type> result = [];

            if (types.Count == 0)
            {
                return result;
            }

            foreach (Classes.Type type in types)
            {
                await using NpgsqlCommand npgsqlCommand_DeleteType = new("DELETE FROM types WHERE id = @id;", npgsqlConnection);
                npgsqlCommand_DeleteType.Parameters.Add("id", NpgsqlDbType.Smallint).Value = type.Id;
                await npgsqlCommand_DeleteType.ExecuteNonQueryAsync();

                result.Add(type);
            }

            return result;
        }
    }
}