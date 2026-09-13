using DiGi.PostgreSQL.Classes;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        /// <summary>
        /// Creates a ConnectionData object from the provided PostgreSQL configuration file.
        /// </summary>
        /// <param name="postgreSQLConfigurationFile">The PostgreSQL configuration file containing connection details.</param>
        /// <returns>A ConnectionData instance if the configuration is valid; otherwise, null.</returns>
        public static ConnectionData? ConnectionData(PostgreSQLConfigurationFile? postgreSQLConfigurationFile)
        {
            if (postgreSQLConfigurationFile is null)
            {
                return null;
            }

            if (string.IsNullOrWhiteSpace(postgreSQLConfigurationFile.Host)
                || string.IsNullOrWhiteSpace(postgreSQLConfigurationFile.Username)
                || string.IsNullOrWhiteSpace(postgreSQLConfigurationFile.Password)
                || string.IsNullOrWhiteSpace(postgreSQLConfigurationFile.Database))
            {
                return null;
            }

            if (postgreSQLConfigurationFile.Port is null)
            {
                return null;
            }

            // The pool settings are optional: a configuration file without the keys answers nulls, keeping the connection string unchanged.
            ConnectionData connectionData = new(postgreSQLConfigurationFile.Host, postgreSQLConfigurationFile.Username, postgreSQLConfigurationFile.Password, postgreSQLConfigurationFile.Database, postgreSQLConfigurationFile.Port.Value)
            {
                MaximumPoolSize = postgreSQLConfigurationFile.MaximumPoolSize,
                PoolTimeout = postgreSQLConfigurationFile.PoolTimeout
            };

            return connectionData;
        }
    }
}