using DiGi.Core.Classes;
using DiGi.PostgreSQL.Classes;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        public static PostgreSQLConfigurationFile? PostgreSQLConfigurationFile(string? path)
        {
            ConfigurationFile? configurationFile = Core.Create.ConfigurationFile(path);
            if (configurationFile is null)
            {
                return null;
            }

            return new PostgreSQLConfigurationFile(configurationFile);
        }
    }
}
