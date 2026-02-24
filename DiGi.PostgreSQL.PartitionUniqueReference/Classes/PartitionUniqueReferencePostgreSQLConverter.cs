using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.PartitionUniqueReference.Delegates;

namespace DiGi.PostgreSQL.PartitionUniqueReference.Classes
{
    public class PartitionUniqueReferencePostgreSQLConverter<TSerializableObject> : PostgreSQLConverter<TSerializableObject> where TSerializableObject : ISerializableObject
    {
        public PartitionUniqueReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }

        public event PartitionUniqueReferenceGeneratingEventHandler PartitionUniqueReferenceReferenceGenerating;
    }
}