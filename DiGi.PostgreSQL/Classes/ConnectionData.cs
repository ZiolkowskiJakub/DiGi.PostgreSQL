using DiGi.Core.Classes;
using DiGi.PostgreSQL.Interfaces;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Classes
{
    public class ConnectionData : SerializableObject, IPostgreSQLObject
    {
        public ConnectionData(string? host, string? username, string? password, string? database, int? port)
        {
            Host = host;
            Username = username;
            Password = password;
            Database = database;
            Port = port;
        }

        public ConnectionData(ConnectionData connectionData, string database)
        {
            Host = connectionData.Host;
            Username = connectionData.Username;
            Password = connectionData.Password;
            Port = connectionData.Port;

            Database = database;
        }

        [JsonInclude, JsonPropertyName("Database")]
        public string? Database { get; set; }

        [JsonInclude, JsonPropertyName("Host")]
        public string? Host { get; set; }

        [JsonInclude, JsonPropertyName("Password")]
        public string? Password { get; set; }

        [JsonInclude, JsonPropertyName("Port")]
        public int? Port { get; set; } = 5432;

        [JsonInclude, JsonPropertyName("Username")]
        public string? Username { get; set; }

        public override string ToString()
        {
            List<string> values = [];
            if(Host != null)
            {
                values.Add($"Host={Host}");
            }

            if (Port != null)
            {
                values.Add($"Port={Port}");
            }

            if (Username != null)
            {
                values.Add($"Username={Username}");
            }

            if (Password != null)
            {
                values.Add($"Password={Password}");
            }

            if (Database != null)
            {
                values.Add($"Database={Database}");
            }


            return string.Join(";", values);
        }

        public ConnectionData GetDefault()
        {
            return new ConnectionData(Host, Username, Password, null, 5432);
        }
    }
}