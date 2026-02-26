using DiGi.PostgreSQL.Classes;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Modify
    {
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