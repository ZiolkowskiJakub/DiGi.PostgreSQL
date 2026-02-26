using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.PartitionUniqueReference.Classes
{
    public class PartitionUniqueReference : SerializableReference
    {
        [JsonInclude, JsonPropertyName("Name")]
        private readonly string? name;

        [JsonInclude, JsonPropertyName("UniqueReference")]
        private readonly IUniqueReference? uniqueReference;

        public PartitionUniqueReference()
            : base()
        {
        }

        public PartitionUniqueReference(JsonObject? jsonObject)
            : base(jsonObject)
        {
        }

        public PartitionUniqueReference(PartitionUniqueReference? partitionUniqueReference)
            : base(partitionUniqueReference)
        {
            if (partitionUniqueReference is not null)
            {
                name = partitionUniqueReference.name;
                uniqueReference = partitionUniqueReference.uniqueReference;
            }
        }

        public PartitionUniqueReference(string? name, IUniqueReference? uniqueReference)
            : base()
        {
            this.name = name;
            this.uniqueReference = uniqueReference;
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
        public IUniqueReference? UniqueReference
        {
            get
            {
                return uniqueReference;
            }
        }

        public override ISerializableObject? Clone()
        {
            return new PartitionUniqueReference(this);
        }

        public override bool Equals(object? obj)
        {
            return obj is PartitionUniqueReference partitionUniqueReference &&
                   base.Equals(obj) &&
                   name == partitionUniqueReference.name &&
                   uniqueReference?.ToString() == partitionUniqueReference.uniqueReference?.ToString();
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string? ToString()
        {
            if (string.IsNullOrWhiteSpace(name) || uniqueReference == null)
            {
                return null;
            }

            return $"{name}{Constants.Reference.Separator}{uniqueReference}";
        }
    }
}