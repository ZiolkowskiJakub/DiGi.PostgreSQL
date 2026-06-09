using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Query
    {
        /// <summary>
        /// Asynchronously retrieves a list of types from the database that are assignable from the specified type.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the query.</param>
        /// <param name="type">The system type used to filter the retrieved types.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of matching <see cref="Classes.Type"/> objects, or null if the connection or type is null.</returns>
        public static async Task<List<Classes.Type>?> TypesAsync(this NpgsqlConnection? npgsqlConnection, System.Type? type)
        {
            if (npgsqlConnection is null || type is null)
            {
                return null;
            }

            string commandText = "SELECT id, name FROM types;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            List<Classes.Type> result = [];

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

                result.Add(new Classes.Type(id, name));
            }

            return result;
        }

        /// <summary>
        /// Asynchronously retrieves a list of types from the database based on the provided type name.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the query.</param>
        /// <param name="name">The name of the type to search for.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of matching <see cref="Classes.Type"/> objects, or null if the connection is null or the name is invalid.</returns>
        public static async Task<List<Classes.Type>?> TypesAsync(this NpgsqlConnection? npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            if (Core.Query.Type(name, false) is not System.Type type)
            {
                return null;
            }

            return await TypesAsync(npgsqlConnection, type);
        }

        /// <summary>
        /// Asynchronously retrieves a list of types from the database filtered by a collection of type identifiers.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to use for the query.</param>
        /// <param name="typeIds">An optional collection of short integers representing the IDs of the types to retrieve.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of matching <see cref="Classes.Type"/> objects, or null if the connection is null.</returns>
        public static async Task<List<Classes.Type>?> TypesAsync(this NpgsqlConnection? npgsqlConnection, IEnumerable<short>? typeIds = null)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            string commandText = "SELECT id, name FROM types;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            List<Classes.Type> result = [];

            while (await npgsqlDataReader.ReadAsync())
            {
                short id = npgsqlDataReader.GetInt16(0);
                if (typeIds is not null && !typeIds.Contains(id))
                {
                    continue;
                }

                string name = npgsqlDataReader.GetString(1);
                if (Core.Query.Type(name, false) is not System.Type type_Temp)
                {
                    continue;
                }

                result.Add(new Classes.Type(id, name));
            }

            return result;
        }
    }
}