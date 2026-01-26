using DiGi.Core.Classes;
using DiGi.PostgreSQL.Interfaces;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Classes
{
    public class ConnectionData : SerializableObject, IPostgreSQLObject
    {
        [JsonInclude, JsonPropertyName("Host")]
        public string Host { get; set; }

        [JsonInclude, JsonPropertyName("Username")]
        public string Username { get; set; }

        [JsonInclude, JsonPropertyName("Password")]
        public string Password { get; set; }

        [JsonInclude, JsonPropertyName("Database")]
        public string Database { get; set; }

        public ConnectionData(string host, string username, string password, string database)
        {
            Host = host;
            Username = username;
            Password = password;
            Database = database;
        }

        public override string ToString()
        {
            return string.Format("Host={0};Username={1};Password={2};Database={3}", Host, Username, Password, Database);
        }
    }
}