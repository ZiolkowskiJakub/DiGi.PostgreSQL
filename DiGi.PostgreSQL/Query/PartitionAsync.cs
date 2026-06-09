using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves a partition by its name from the database.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection to use for the query.</param>
        /// <param name="name">The name of the partition to retrieve.</param>
        /// <returns>A <see cref="Partition"/> object if found; otherwise, null.</returns>
        public static async Task<Partition?> PartitionAsync(this NpgsqlConnection? npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            string commandText = @"
                SELECT id, data_type
                FROM partitions
                WHERE name = @name;
                ";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

            npgsqlCommand.Parameters.AddWithValue("name", name);

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();
            if (await npgsqlDataReader.ReadAsync())
            {
                short id = npgsqlDataReader.GetInt16(0);
                short dataType = npgsqlDataReader.GetInt16(1);

                return new Partition(id, name, (Enums.DataType)dataType);
            }

            return null;
        }

        /// <summary>
        /// Asynchronously retrieves a partition by its ID from the database.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection to use for the query.</param>
        /// <param name="partitionId">The unique identifier of the partition to retrieve.</param>
        /// <returns>A <see cref="Partition"/> object if found; otherwise, null.</returns>
        public static async Task<Partition?> PartitionAsync(this NpgsqlConnection? npgsqlConnection, short partitionId)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            string commandText = @"
                SELECT name, data_type
                FROM partitions
                WHERE id = @id;
                ";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

            npgsqlCommand.Parameters.AddWithValue("id", partitionId);

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();
            if (await npgsqlDataReader.ReadAsync())
            {
                string name = npgsqlDataReader.GetString(0);
                short dataType = npgsqlDataReader.GetInt16(1);

                return new Partition(partitionId, name, (Enums.DataType)dataType);
            }

            return null;
        }
    }
}