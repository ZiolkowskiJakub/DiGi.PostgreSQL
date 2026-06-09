using DiGi.Core.Classes;
using DiGi.PostgreSQL.Interfaces;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.PartitionUniqueReference.Classes
{
    /// <summary>
    /// Represents a type definition with an ID and name, used for partitioning unique references.
    /// </summary>
    public class Type : SerializableObject, IPostgreSQLSerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Type"/> class from a JSON object.
        /// </summary>
        /// <param name="jsonObject">The JSON object containing the type definition data.</param>
        public Type(JsonObject jsonObject)
            : base(jsonObject)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Type"/> class by copying another instance.
        /// </summary>
        /// <param name="type">The type instance to copy.</param>
        public Type(Type type)
            : base(type)
        {
            if (type is not null)
            {
                Id = type.Id;
                Name = type.Name;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Type"/> class with the specified ID and name.
        /// </summary>
        /// <param name="id">The unique identifier of the type.</param>
        /// <param name="name">The name of the type.</param>
        public Type(short id, string name)
        {
            Id = id;
            Name = name;
        }

        /// <summary>
        /// Gets the unique identifier of the type.
        /// </summary>
        [JsonInclude, JsonPropertyName("Id")]
        public short Id { get; }

        /// <summary>
        /// Gets the name of the type.
        /// </summary>
        [JsonInclude, JsonPropertyName("Name")]
        public string? Name { get; }
    }
}