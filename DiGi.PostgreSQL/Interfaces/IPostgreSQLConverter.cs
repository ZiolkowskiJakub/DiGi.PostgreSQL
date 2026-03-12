using DiGi.Core.Interfaces;

namespace DiGi.PostgreSQL.Interfaces
{
    public interface IPostgreSQLConverter : IPostgreSQLObject
    {
    }

    public interface IPostgreSQLConverter<TSerializableObject> : IPostgreSQLConverter where TSerializableObject : ISerializableObject
    {
    }
}