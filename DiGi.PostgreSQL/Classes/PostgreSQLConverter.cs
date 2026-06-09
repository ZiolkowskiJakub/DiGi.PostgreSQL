using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Interfaces;

namespace DiGi.PostgreSQL.Classes
{
    /// <summary>
    /// Base class for converting objects to and from PostgreSQL database format.
    /// </summary>
    /// <typeparam name="TObject">The type of object being converted, which must implement IObject.</typeparam>
    public abstract class PostgreSQLConverter<TObject> : IPostgreSQLConverter<TObject> where TObject : IObject
    {
        /// <summary>
        /// Gets or sets the connection data used by the converter.
        /// </summary>
        public ConnectionData? ConnectionData { get; set; }

        /// <summary>
        /// Initializes a new instance of the PostgreSQLConverter class.
        /// </summary>
        /// <param name="connectionData">The connection data to be used for database operations.</param>
        public PostgreSQLConverter(ConnectionData? connectionData)
        {
            ConnectionData = connectionData;
        }
    }
}