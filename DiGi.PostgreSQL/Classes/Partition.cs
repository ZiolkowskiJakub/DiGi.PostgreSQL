using DiGi.Core.Classes;
using DiGi.PostgreSQL.Enums;
using DiGi.PostgreSQL.Interfaces;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Classes
{
    public class Partition : SerializableObject, IPostgreSQLSerializableObject
    {
        public Partition(JsonObject jsonObject)
            : base(jsonObject)
        {
        }

        public Partition(Partition partition)
            : base(partition)
        {
            if (partition is not null)
            {
                Id = partition.Id;
                Name = partition.Name;
                DataType = partition.DataType;
            }
        }

        public Partition(short id, string name, DataType dataType)
        {
            Id = id;
            Name = name;
            DataType = dataType;
        }

        [JsonInclude, JsonPropertyName("DataType")]
        public DataType DataType { get; }

        [JsonInclude, JsonPropertyName("Id")]
        public short Id { get; }

        [JsonInclude, JsonPropertyName("Name")]
        public string? Name { get; }
    }
}