using DiGi.Core.Classes;
using DiGi.PostgreSQL.Classes;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        /// <summary>
        /// Creates a new instance of a PostgreSQL configuration file from the specified path.
        /// </summary>
        /// <param name="path">The path to the configuration file.</param>
        /// <returns>A <see cref="PostgreSQLConfigurationFile"/> if successful; otherwise, null.</returns>
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