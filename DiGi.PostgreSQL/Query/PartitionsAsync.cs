using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves all partitions from the database.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection to use for the query.</param>
        /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of partitions, or null if the connection is null.</returns>
        public static async Task<List<Partition>?> PartitionsAsync(this NpgsqlConnection? npgsqlConnection, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            string commandText = "SELECT id, name, data_type FROM partitions;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync(cancellationToken);

            List<Partition> result = [];

            while (await npgsqlDataReader.ReadAsync(cancellationToken))
            {
                short id = npgsqlDataReader.GetInt16(0);
                string name = npgsqlDataReader.GetString(1);
                short dataType = npgsqlDataReader.GetInt16(2);

                result.Add(new Partition(id, name, (Enums.DataType)dataType));
            }

            return result;
        }

        /// <summary>
        /// Asynchronously retrieves partitions from the database based on a collection of partition identifiers.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection to use for the query.</param>
        /// <param name="partitionIds">A collection of short integers representing the IDs of the partitions to retrieve.</param>
        /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of matching partitions, or null if the connection or partitionIds is null.</returns>
        public static async Task<List<Partition>?> PartitionsAsync(this NpgsqlConnection? npgsqlConnection, IEnumerable<short>? partitionIds, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || partitionIds is null)
            {
                return null;
            }

            if (!partitionIds.Any())
            {
                return [];
            }

            // Using ANY(@parameter) is more efficient and cleaner than building an IN clause
            string commandText = "SELECT id, name, data_type FROM partitions WHERE id = ANY(@ids);";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("ids", partitionIds.ToArray());

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync(cancellationToken);

            List<Partition> result = [];

            while (await npgsqlDataReader.ReadAsync())
            {
                short id = npgsqlDataReader.GetInt16(0);
                string name = npgsqlDataReader.GetString(1);
                short dataType = npgsqlDataReader.GetInt16(2);

                result.Add(new Partition(id, name, (Enums.DataType)dataType));
            }

            return result;
        }

        /// <summary>
        /// Asynchronously retrieves partitions from the database based on a collection of partition names.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection to use for the query.</param>
        /// <param name="names">A collection of strings representing the names of the partitions to retrieve.</param>
        /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of matching partitions, or null if the connection or names is null.</returns>
        public static async Task<List<Partition>?> PartitionsAsync(this NpgsqlConnection? npgsqlConnection, IEnumerable<string>? names, CancellationToken cancellationToken = default)
        {
            if (npgsqlConnection is null || names is null)
            {
                return null;
            }

            if (!names.Any())
            {
                return [];
            }

            // Using ANY(@parameter) is more efficient and cleaner than building an IN clause
            string commandText = "SELECT id, name, data_type FROM partitions WHERE name = ANY(@names);";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("names", names.ToArray());

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync(cancellationToken);

            List<Partition> result = [];

            while (await npgsqlDataReader.ReadAsync())
            {
                short id = npgsqlDataReader.GetInt16(0);
                string name = npgsqlDataReader.GetString(1);
                short dataType = npgsqlDataReader.GetInt16(2);

                result.Add(new Partition(id, name, (Enums.DataType)dataType));
            }

            return result;
        }
    }
}