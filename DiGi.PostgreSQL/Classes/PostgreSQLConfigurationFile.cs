using DiGi.Core.Classes;
using DiGi.PostgreSQL.Interfaces;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Classes
{
    public class PostgreSQLConfigurationFile : ConfigurationFile, IPostgreSQLSerializableObject
    {
        public PostgreSQLConfigurationFile()
            : base()
        {
        }

        public PostgreSQLConfigurationFile(JsonObject? jsonObject)
            : base(jsonObject)
        {
        }

        public PostgreSQLConfigurationFile(ConfigurationFile? configurationFile)
            : base(configurationFile)
        {
        }

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