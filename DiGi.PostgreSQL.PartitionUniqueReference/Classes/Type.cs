using DiGi.Core.Classes;
using DiGi.PostgreSQL.Interfaces;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.PartitionUniqueReference.Classes
{
    public class Type : SerializableObject, IPostgreSQLSerializableObject
    {
        public Type(JsonObject jsonObject)
            : base(jsonObject)
        {
        }

        public Type(Type type)
            : base(type)
        {
            if (type is not null)
            {
                Id = type.Id;
                Name = type.Name;
            }
        }

        public Type(short id, string name)
        {
            Id = id;
            Name = name;
        }

        [JsonInclude, JsonPropertyName("Id")]
        public short Id { get; }

        [JsonInclude, JsonPropertyName("Name")]
        public string? Name { get; }
    }
}