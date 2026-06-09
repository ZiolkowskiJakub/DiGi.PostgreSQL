using DiGi.PostgreSQL.Classes;
using Npgsql;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        /// <summary>
        /// Creates a new NpgsqlConnection using the provided connection data.
        /// </summary>
        /// <param name="connectionData">The connection data containing host, username, and password.</param>
        /// <returns>An instance of NpgsqlConnection if the connection data is valid; otherwise, null.</returns>
        public static NpgsqlConnection? NpgsqlConnection(ConnectionData? connectionData)
        {
            if (connectionData is null)
            {
                return null;
            }

            if (string.IsNullOrWhiteSpace(connectionData.Host)
                || string.IsNullOrWhiteSpace(connectionData.Username)
                || string.IsNullOrWhiteSpace(connectionData.Password))
            {
                return null;
            }

            return new NpgsqlConnection(connectionData.ToString());
        }

        /// <summary>
        /// Creates a new NpgsqlConnection using the provided PostgreSQL configuration file.
        /// </summary>
        /// <param name="postgreSQLConfigurationFile">The configuration file containing connection settings.</param>
        /// <returns>An instance of NpgsqlConnection if the configuration is valid; otherwise, null.</returns>
        public static NpgsqlConnection? NpgsqlConnection(PostgreSQLConfigurationFile? postgreSQLConfigurationFile)
        {
            return NpgsqlConnection(ConnectionData(postgreSQLConfigurationFile));
        }
    }
}