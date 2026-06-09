using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves a list of partitions that are assignable from the specified type.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection to use for the query.</param>
        /// <param name="type">The system type used to filter the partitions.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <see cref="Partition"/> objects, or null if the connection or type is null.</returns>
        public static async Task<List<Partition>?> PartitionsAsync(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return null;
            }

            string commandText = "SELECT id, name, data_type FROM partitions;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            List<Partition> result = [];

            while (await npgsqlDataReader.ReadAsync())
            {
                string name = npgsqlDataReader.GetString(1);
                if (Core.Query.Type(name, false) is not System.Type type_Temp)
                {
                    continue;
                }

                if (!type.IsAssignableFrom(type_Temp))
                {
                    continue;
                }

                short id = npgsqlDataReader.GetInt16(0);
                short dataType = npgsqlDataReader.GetInt16(2);

                result.Add(new Partition(id, name, (Enums.DataType)dataType));
            }

            return result;
        }

        /// <summary>
        /// Asynchronously retrieves a list of partitions associated with the specified type name.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection to use for the query.</param>
        /// <param name="name">The name of the type used to filter the partitions.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <see cref="Partition"/> objects, or null if the connection is null or the name is invalid.</returns>
        public static async Task<List<Partition>?> PartitionsAsync(this NpgsqlConnection? npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            if (Core.Query.Type(name, false) is not System.Type type)
            {
                return null;
            }

            return await PartitionsAsync(npgsqlConnection, type);
        }
    }
}