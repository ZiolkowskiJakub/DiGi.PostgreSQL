using DiGi.PostgreSQL.Classes;
using Npgsql;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static bool IsAvailable(this ConnectionData connectionData)
        {
            try
            {
                using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(connectionData);
                if (npgsqlConnection is null)
                {
                    return false;
                }

                npgsqlConnection.Open();

                using NpgsqlCommand npgsqlCommand = new("SELECT 1", npgsqlConnection);
                npgsqlCommand.ExecuteScalar();

                return true;
            }
            catch
            {
            }

            return false;
        }
    }
}