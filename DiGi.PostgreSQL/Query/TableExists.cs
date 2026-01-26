using DiGi.PostgreSQL.Classes;
using Npgsql;
using System;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static bool TableExists(this NpgsqlConnection? npgsqlConnection, string tableName)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            using NpgsqlCommand npgsqlCommand = new("SELECT to_regclass(@tableName);", npgsqlConnection);

            // Include schema if not public!
            npgsqlCommand.Parameters.AddWithValue("tableName", $"public.{tableName}");

            return npgsqlCommand.ExecuteScalar() != DBNull.Value;
        }

        public static bool TableExists(this ConnectionData? connectionData, string tableName)
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

            return TableExists(npgsqlConnection, tableName);
        }
    }
}