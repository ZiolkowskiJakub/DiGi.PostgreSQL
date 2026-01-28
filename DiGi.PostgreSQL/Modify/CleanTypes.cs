using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<List<short>?> CleanTypes(NpgsqlConnection? npgsqlConnection, IEnumerable<short>? typeIds = null)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            typeIds ??= (await Query.TypeIds(npgsqlConnection))?.Keys;

            if (typeIds is null)
            {
                return null;
            }

            List<short> result = [];

            if (!typeIds.Any())
            {
                return result;
            }

            foreach (short typeId in typeIds)
            {
                // Check if the partition is now empty
                await using NpgsqlCommand npgsqlCommand_Check = new("SELECT NOT EXISTS(SELECT 1 FROM objects WHERE type_id = @type_id);", npgsqlConnection);
                npgsqlCommand_Check.Parameters.Add("type_id", NpgsqlDbType.Smallint).Value = typeId;

                bool isEmpty = (bool)(await npgsqlCommand_Check.ExecuteScalarAsync() ?? false);
                if (isEmpty)
                {
                    // 1. Remove from types first (Metadata)
                    await using NpgsqlCommand npgsqlCommand_DeleteType = new("DELETE FROM types WHERE id = @type_id;", npgsqlConnection);
                    npgsqlCommand_DeleteType.Parameters.Add("type_id", NpgsqlDbType.Smallint).Value = typeId;
                    await npgsqlCommand_DeleteType.ExecuteNonQueryAsync();

                    // 2. Drop the physical partition table
                    await using NpgsqlCommand npgsqlCommand_DropTable = new($"DROP TABLE IF EXISTS objects_type_{typeId};", npgsqlConnection);
                    await npgsqlCommand_DropTable.ExecuteNonQueryAsync();

                    result.Add(typeId);
                }
            }

            return result;
        }
    }
}