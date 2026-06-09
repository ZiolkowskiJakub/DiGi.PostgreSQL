using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Modify
    {
        /// <summary>
        /// Updates or creates a type ID based on the provided name.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance used to perform the operation.</param>
        /// <param name="name">The name of the type to update or create.</param>
        /// <returns>A task that represents the asynchronous operation, containing the updated or created <see cref="Classes.Type"/> object, or null if the operation failed or inputs were invalid.</returns>
        public static async Task<Classes.Type?> UpdateTypeIdAsync(this NpgsqlConnection? npgsqlConnection, string? name)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            Classes.Type? result = await Query.TypeAsync(npgsqlConnection, name);
            if (result is null)
            {
                string commandText = @"
                INSERT INTO types (name)
                VALUES (@name)
                RETURNING id;";

                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                npgsqlCommand.Parameters.AddWithValue("name", name);

                short? typeId = (short?)await npgsqlCommand.ExecuteScalarAsync();
                if (typeId is null)
                {
                    return null;
                }

                result = await Query.TypeAsync(npgsqlConnection, name);
            }

            return result;
        }
    }
}