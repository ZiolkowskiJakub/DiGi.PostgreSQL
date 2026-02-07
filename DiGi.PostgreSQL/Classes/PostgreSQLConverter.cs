using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Interfaces;

namespace DiGi.PostgreSQL.Classes
{
    public abstract class PostgreSQLConverter<TSerializableObject> : IPostgreSQLConverter<TSerializableObject> where TSerializableObject : ISerializableObject
    {
        public ConnectionData? ConnectionData { get; set; }

        public PostgreSQLConverter(ConnectionData? connectionData)
        {
            ConnectionData = connectionData;
        }
    }
}