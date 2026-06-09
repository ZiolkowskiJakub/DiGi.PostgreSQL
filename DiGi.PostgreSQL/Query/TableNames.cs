using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Collections.Generic;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Retrieves a list of table names from the public schema of the PostgreSQL database.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection instance used to execute the query.</param>
        /// <returns>A list of strings containing the table names, or null if the connection is null.</returns>
        public static List<string>? TableNames(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            string sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';";

            using NpgsqlCommand npgsqlCommand = new(sql, npgsqlConnection);
            using NpgsqlDataReader reader = npgsqlCommand.ExecuteReader();

            List<string> result = [];
            while (reader.Read())
            {
                result.Add(reader.GetString(0));
            }

            return result;
        }

        /// <summary>
        /// Retrieves a list of table names from the public schema of the PostgreSQL database using provided connection data.
        /// </summary>
        /// <param name="connectionData">The connection data used to establish a database connection.</param>
        /// <returns>A list of strings containing the table names, or null if the connection cannot be established.</returns>
        public static List<string>? TableNames(this ConnectionData? connectionData)
        {
            if (Create.NpgsqlConnection(connectionData) is not NpgsqlConnection npgsqlConnection)
            {
                return null;
            }

            return TableNames(npgsqlConnection);
        }
    }
}