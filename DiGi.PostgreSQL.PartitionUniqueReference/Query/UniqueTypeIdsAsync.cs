using DiGi.PostgreSQL.Classes;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Query
    {
        /// <summary>
        /// Retrieves all unique type_id values present in a specific partition.
        /// </summary>
        public static async Task<HashSet<short>?> UniqueTypeIdsAsync(this NpgsqlConnection? npgsqlConnection, Partition? partition)
        {
            if (npgsqlConnection is null || partition is null)
            {
                return null;
            }

            // The query leverages the composite index (partition_id, type_id, unique_id)
            string commandText = $@"
                SELECT DISTINCT type_id
                FROM objects_{(int)partition.DataType}
                WHERE partition_id = @partition_id";

            HashSet<short> result = [];

            try
            {
                await using NpgsqlCommand npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);
                npgsqlCommand.Parameters.AddWithValue("partition_id", partition.Id);

                await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

                while (await npgsqlDataReader.ReadAsync())
                {
                    result.Add(npgsqlDataReader.GetInt16(0));
                }

                return result;
            }
            catch (NpgsqlException ex)
            {
                Console.WriteLine($"Postgres Error (UniqueTypeIdsAsync): {ex.Message}");
                return null;
            }
        }
    }
}