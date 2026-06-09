using DiGi.PostgreSQL.Classes;
using Npgsql;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Checks if the PostgreSQL database is available using the provided connection data.
        /// </summary>
        /// <param name="connectionData">The connection data used to establish a connection to the database.</param>
        /// <returns>True if the database is available and reachable; otherwise, false.</returns>
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

        /// <summary>
        /// Checks if the PostgreSQL database is available using the provided configuration file.
        /// </summary>
        /// <param name="postgreSQLConfigurationFile">The configuration file containing connection details.</param>
        /// <returns>True if the database is available and reachable; otherwise, false.</returns>
        public static bool IsAvailable(this PostgreSQLConfigurationFile postgreSQLConfigurationFile)
        {
            ConnectionData? connectionData = Create.ConnectionData(postgreSQLConfigurationFile);
            if (connectionData is null)
            {
                return false;
            }

            return IsAvailable(connectionData);
        }
    }
}