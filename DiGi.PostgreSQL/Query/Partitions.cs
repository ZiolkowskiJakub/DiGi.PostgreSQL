using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<List<Partition>?> Partitions(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            string commandText = "SELECT id, name, data_type FROM partitions;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

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

        public static async Task<List<Partition>?> Partitions(this NpgsqlConnection? npgsqlConnection, IEnumerable<short>? partitionIds)
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

            await using NpgsqlCommand npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("ids", partitionIds.ToArray());

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

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

        public static async Task<List<Partition>?> Partitions(this NpgsqlConnection? npgsqlConnection, IEnumerable<string>? names)
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

            await using NpgsqlCommand npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.AddWithValue("names", names.ToArray());

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

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