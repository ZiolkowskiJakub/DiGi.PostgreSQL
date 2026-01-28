using DiGi.PostgreSQL.Classes;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
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

            return new ConnectionData(postgreSQLConfigurationFile.Host, postgreSQLConfigurationFile.Username, postgreSQLConfigurationFile.Password, postgreSQLConfigurationFile.Database, postgreSQLConfigurationFile.Port.Value);
        }
    }
}