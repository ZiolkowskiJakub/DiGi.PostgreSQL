namespace DiGi.PostgreSQL.Constants
{
    /// <summary>
    /// Gets the list of all configuration key names.
    /// </summary>
    public static class Names
    {
        /// <summary>
        /// Contains configuration key names for PostgreSQL configuration files.
        /// </summary>
        public static class PostgreSQLConfigurationFile
        {
            /// <summary>
            /// PostgresSQL database host.
            /// </summary>
            public const string Host = "HOST";

            /// <summary>
            /// PostgresSQL database port.
            /// </summary>
            public const string Port = "PORT";

            /// <summary>
            /// PostgresSQL username.
            /// </summary>
            public const string Username = "USERNAME";

            /// <summary>
            /// PostgresSQL password.
            /// </summary>
            public const string Password = "PASSWORD";

            /// <summary>
            /// PostgresSQL database name.
            /// </summary>
            public const string Database = "DATABASE";

            /// <summary>
            /// PostgresSQL database tablespace.
            /// </summary>
            public const string Tablespace = "TABLESPACE";

            /// <summary>
            /// PostgresSQL database directory.
            /// </summary>
            public const string Directory = "DIRECTORY";
        }
    }
}