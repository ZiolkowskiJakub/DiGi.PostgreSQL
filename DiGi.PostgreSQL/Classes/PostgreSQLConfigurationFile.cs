using DiGi.Core.Classes;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Classes
{
    public class PostgreSQLConfigurationFile : ConfigurationFile
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
                return GetValue<string>(Constans.Names.PostgreSQLConfigurationFile.Database);
            }

            set
            {
                Add(Constans.Names.PostgreSQLConfigurationFile.Database, value);
            }
        }

        [JsonIgnore]
        public string? Host
        {
            get
            {
                return GetValue<string>(Constans.Names.PostgreSQLConfigurationFile.Host);
            }

            set
            {
                Add(Constans.Names.PostgreSQLConfigurationFile.Host, value);
            }
        }

        [JsonIgnore]
        public string? Password
        {
            get
            {
                return GetValue<string>(Constans.Names.PostgreSQLConfigurationFile.Password);
            }

            set
            {
                Add(Constans.Names.PostgreSQLConfigurationFile.Password, value);
            }
        }

        [JsonIgnore]
        public int? Port
        {
            get
            {
                return GetValue<int?>(Constans.Names.PostgreSQLConfigurationFile.Port);
            }

            set
            {
                Add(Constans.Names.PostgreSQLConfigurationFile.Port, value);
            }
        }

        [JsonIgnore]
        public string? Username
        {
            get
            {
                return GetValue<string>(Constans.Names.PostgreSQLConfigurationFile.Username);
            }

            set
            {
                Add(Constans.Names.PostgreSQLConfigurationFile.Username, value);
            }
        }
    }
}