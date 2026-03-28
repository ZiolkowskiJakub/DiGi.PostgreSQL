using DiGi.PostgreSQL.Classes;
using Npgsql;
using System;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static async Task<bool> TableExistsAsync(this NpgsqlConnection? npgsqlConnection, string tableName)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            // Explicitly cast to text so Npgsql can handle the return value
            using NpgsqlCommand npgsqlCommand = new("SELECT to_regclass(@tableName)::text;", npgsqlConnection);

            // It's safer to use the parameter name without @ in AddWithValue,
            // though Npgsql handles both.
            npgsqlCommand.Parameters.AddWithValue("tableName", $"public.{tableName}");

            object? result = await npgsqlCommand.ExecuteScalarAsync();

            // If the table doesn't exist, to_regclass returns NULL (DBNull.Value in C#)
            return result != null && result != DBNull.Value;
        }

        public static async Task<bool> TableExistsAsync(this ConnectionData? connectionData, string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            if (Create.NpgsqlConnection(connectionData) is not NpgsqlConnection npgsqlConnection)
            {
                return false;
            }

            npgsqlConnection.Open();

            return await TableExistsAsync(npgsqlConnection, tableName);
        }
    }
}