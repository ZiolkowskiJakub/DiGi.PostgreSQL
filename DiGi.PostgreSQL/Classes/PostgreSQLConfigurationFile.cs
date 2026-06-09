using DiGi.Core.Classes;
using DiGi.PostgreSQL.Interfaces;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Classes
{
    /// <summary>
    /// Represents a configuration file specifically for PostgreSQL database settings.
    /// </summary>
    public class PostgreSQLConfigurationFile : ConfigurationFile, IPostgreSQLSerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PostgreSQLConfigurationFile"/> class.
        /// </summary>
        public PostgreSQLConfigurationFile()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgreSQLConfigurationFile"/> class using a JSON object.
        /// </summary>
        /// <param name="jsonObject">The JSON object containing the configuration data.</param>
        public PostgreSQLConfigurationFile(JsonObject? jsonObject)
            : base(jsonObject)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PostgreSQLConfigurationFile"/> class based on an existing configuration file.
        /// </summary>
        /// <param name="configurationFile">The source configuration file to copy settings from.</param>
        public PostgreSQLConfigurationFile(ConfigurationFile? configurationFile)
            : base(configurationFile)
        {
        }

        /// <summary>
        /// Gets or sets the name of the PostgreSQL database.
        /// </summary>
        [JsonIgnore]
        public string? Database
        {
            get
            {
                return GetValue<string>(Constants.Names.PostgreSQLConfigurationFile.Database);
            }

            set
            {
                Add(Constants.Names.PostgreSQLConfigurationFile.Database, value);
            }
        }

        /// <summary>
        /// Gets or sets the directory path associated with the PostgreSQL configuration.
        /// </summary>
        [JsonIgnore]
        public string? Directory
        {
            get
            {
                return GetValue<string>(Constants.Names.PostgreSQLConfigurationFile.Directory);
            }

            set
            {
                Add(Constants.Names.PostgreSQLConfigurationFile.Directory, value);
            }
        }

        /// <summary>
        /// Gets or sets the host address of the PostgreSQL server.
        /// </summary>
        [JsonIgnore]
        public string? Host
        {
            get
            {
                return GetValue<string>(Constants.Names.PostgreSQLConfigurationFile.Host);
            }

            set
            {
                Add(Constants.Names.PostgreSQLConfigurationFile.Host, value);
            }
        }

        /// <summary>
        /// Gets or sets the password for the PostgreSQL user.
        /// </summary>
        [JsonIgnore]
        public string? Password
        {
            get
            {
                return GetValue<string>(Constants.Names.PostgreSQLConfigurationFile.Password);
            }

            set
            {
                Add(Constants.Names.PostgreSQLConfigurationFile.Password, value);
            }
        }

        /// <summary>
        /// Gets or sets the port number used to connect to the PostgreSQL server.
        /// </summary>
        [JsonIgnore]
        public int? Port
        {
            get
            {
                return GetValue<int?>(Constants.Names.PostgreSQLConfigurationFile.Port);
            }

            set
            {
                Add(Constants.Names.PostgreSQLConfigurationFile.Port, value);
            }
        }

        /// <summary>
        /// Gets or sets the tablespace name for the PostgreSQL database.
        /// </summary>
        [JsonIgnore]
        public string? Tablespace
        {
            get
            {
                return GetValue<string>(Constants.Names.PostgreSQLConfigurationFile.Tablespace);
            }

            set
            {
                Add(Constants.Names.PostgreSQLConfigurationFile.Tablespace, value);
            }
        }

        /// <summary>
        /// Gets or sets the username for the PostgreSQL connection.
        /// </summary>
        [JsonIgnore]
        public string? Username
        {
            get
            {
                return GetValue<string>(Constants.Names.PostgreSQLConfigurationFile.Username);
            }

            set
            {
                Add(Constants.Names.PostgreSQLConfigurationFile.Username, value);
            }
        }
    }
}