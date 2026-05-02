using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Interfaces;

namespace DiGi.PostgreSQL.Classes
{
    public abstract class PostgreSQLConverter<TObject> : IPostgreSQLConverter<TObject> where TObject : IObject
    {
        public ConnectionData? ConnectionData { get; set; }

        public PostgreSQLConverter(ConnectionData? connectionData)
        {
            ConnectionData = connectionData;
        }
    }
}