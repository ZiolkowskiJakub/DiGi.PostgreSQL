using DiGi.Core.Interfaces;

namespace DiGi.PostgreSQL.Interfaces
{
    public interface IPostgreSQLConverter : IPostgreSQLObject
    {
    }

    public interface IPostgreSQLConverter<TObject> : IPostgreSQLConverter where TObject : IObject
    {
    }
}