using DiGi.Core.Interfaces;

namespace DiGi.PostgreSQL.Interfaces
{
    /// <summary>
    /// Defines the base contract for a PostgreSQL converter.
    /// </summary>
    public interface IPostgreSQLConverter : IPostgreSQLObject
    {
    }

    /// <summary>
    /// Defines the contract for a PostgreSQL converter that handles a specific object type.
    /// </summary>
    /// <typeparam name="TObject">The type of object to be converted, which must implement <see cref="IObject"/>.</typeparam>
    public interface IPostgreSQLConverter<TObject> : IPostgreSQLConverter where TObject : IObject
    {
    }
}