using DiGi.Core.Interfaces;

namespace DiGi.PostgreSQL.Interfaces
{
    public interface IPostgreSQLConverter<TSerializableObject> : IPostgreSQLObject where TSerializableObject : ISerializableObject
    {
    }
}