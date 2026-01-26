using DiGi.PostgreSQL.Classes;
using Npgsql;
using System.Collections.Generic;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
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