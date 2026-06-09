using DiGi.Core.Interfaces;

namespace DiGi.PostgreSQL.Interfaces
{
    /// <summary>
    /// Defines the contract for objects that are compatible with PostgreSQL storage and can be serialized to and from JSON.
    /// </summary>
    public interface IPostgreSQLSerializableObject : IPostgreSQLObject, ISerializableObject
    {
    }
}