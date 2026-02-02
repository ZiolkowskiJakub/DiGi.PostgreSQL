using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.PartitionReference.Classes
{
    public class PartitionReference : SerializableReference
    {
        [JsonInclude, JsonPropertyName("Name")]
        private readonly string? name;

        [JsonInclude, JsonPropertyName("UniqueId")]
        private readonly string? uniqueId;

        public PartitionReference()
            : base()
        {
        }

        public PartitionReference(JsonObject? jsonObject)
            : base(jsonObject)
        {
        }

        public PartitionReference(PartitionReference? partitionReference)
            : base(partitionReference)
        {
            if (partitionReference is not null)
            {
                name = partitionReference.name;
                uniqueId = partitionReference.uniqueId;
            }
        }

        public PartitionReference(string name, string uniqueId)
            : base()
        {
            this.name = name;
            this.uniqueId = uniqueId;
        }

        [JsonIgnore]
        public string? Name
        {
            get
            {
                return name;
            }
        }

        [JsonIgnore]
        public string? UniqueId
        {
            get
            {
                return uniqueId;
            }
        }

        public override ISerializableObject? Clone()
        {
            return new PartitionReference(this);
        }

        public override bool Equals(object? obj)
        {
            return obj is PartitionReference reference &&
                   base.Equals(obj) &&
                   name == reference.name &&
                   uniqueId == reference.uniqueId;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string? ToString()
        {
            if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(uniqueId))
            {
                return null;
            }

            return $"{name}{Constans.Reference.Separator}{uniqueId}";
        }
    }
}