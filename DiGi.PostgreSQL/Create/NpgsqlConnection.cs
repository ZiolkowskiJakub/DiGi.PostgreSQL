using DiGi.PostgreSQL.Classes;
using Npgsql;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        public static NpgsqlConnection? NpgsqlConnection(ConnectionData? connectionData)
        {
            if (connectionData is null)
            {
                return null;
            }

            if (string.IsNullOrWhiteSpace(connectionData.Host)
                || string.IsNullOrWhiteSpace(connectionData.Username)
                || string.IsNullOrWhiteSpace(connectionData.Password)
                || string.IsNullOrWhiteSpace(connectionData.Database))
            {
                return null;
            }

            return new NpgsqlConnection(connectionData.ToString());
        }
    }
}