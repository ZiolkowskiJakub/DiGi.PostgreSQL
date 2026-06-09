using DiGi.Core.Classes;
using DiGi.PostgreSQL.Interfaces;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Classes
{
    /// <summary>
    /// Represents the connection settings required to establish a connection to a PostgreSQL database.
    /// </summary>
    public class ConnectionData : SerializableObject, IPostgreSQLSerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionData"/> class with specified connection details.
        /// </summary>
        /// <param name="host">The server host address.</param>
        /// <param name="username">The user name for authentication.</param>
        /// <param name="password">The password for authentication.</param>
        /// <param name="database">The name of the database to connect to.</param>
        /// <param name="port">The port number of the PostgreSQL server.</param>
        public ConnectionData(string? host, string? username, string? password, string? database, int? port)
        {
            Host = host;
            Username = username;
            Password = password;
            Database = database;
            Port = port;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionData"/> class based on an existing connection configuration but with a different database.
        /// </summary>
        /// <param name="connectionData">The source connection data containing host, username, password, and port.</param>
        /// <param name="database">The name of the database to connect to.</param>
        public ConnectionData(ConnectionData connectionData, string database)
        {
            Host = connectionData.Host;
            Username = connectionData.Username;
            Password = connectionData.Password;
            Port = connectionData.Port;

            Database = database;
        }

        /// <summary>
        /// Gets or sets the name of the PostgreSQL database.
        /// </summary>
        [JsonInclude, JsonPropertyName("Database")]
        public string? Database { get; set; }

        /// <summary>
        /// Gets or sets the server host address.
        /// </summary>
        [JsonInclude, JsonPropertyName("Host")]
        public string? Host { get; set; }

        /// <summary>
        /// Gets or sets the password for authentication.
        /// </summary>
        [JsonInclude, JsonPropertyName("Password")]
        public string? Password { get; set; }

        /// <summary>
        /// Gets or sets the port number of the PostgreSQL server. Defaults to 5432.
        /// </summary>
        [JsonInclude, JsonPropertyName("Port")]
        public int? Port { get; set; } = 5432;

        /// <summary>
        /// Gets or sets the user name for authentication.
        /// </summary>
        [JsonInclude, JsonPropertyName("Username")]
        public string? Username { get; set; }

        /// <summary>
        /// Returns a string representation of the connection data in a semicolon-separated format.
        /// </summary>
        /// <returns>A formatted string containing the connection details.</returns>
        public override string ToString()
        {
            List<string> values = [];
            if (Host != null)
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

        /// <summary>
        /// Creates a new <see cref="ConnectionData"/> instance with default settings, using the current host and credentials but resetting the database and port.
        /// </summary>
        /// <returns>A <see cref="ConnectionData"/> object initialized with default values.</returns>
        public ConnectionData GetDefault()
        {
            return new ConnectionData(Host, Username, Password, null, 5432);
        }
    }
}