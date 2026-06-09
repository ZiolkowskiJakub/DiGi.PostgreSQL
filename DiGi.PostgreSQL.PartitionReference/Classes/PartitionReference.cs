using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.PartitionReference.Classes
{
    /// <summary>Represents a reference to a partition.</summary>
    public class PartitionReference : SerializableReference
    {
        [JsonInclude, JsonPropertyName("Name")]
        private readonly string? name;

        [JsonInclude, JsonPropertyName("UniqueId")]
        private readonly string? uniqueId;

        /// <summary>Initializes a new instance of the <see cref="PartitionReference"/> class.</summary>
        public PartitionReference()
            : base()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="PartitionReference"/> class from a JSON object.</summary>
        /// <param name="jsonObject">The JSON object to initialize from.</param>
        public PartitionReference(JsonObject? jsonObject)
            : base(jsonObject)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="PartitionReference"/> class by copying another reference.</summary>
        /// <param name="partitionReference">The source reference to copy.</param>
        public PartitionReference(PartitionReference? partitionReference)
            : base(partitionReference)
        {
            if (partitionReference is not null)
            {
                name = partitionReference.name;
                uniqueId = partitionReference.uniqueId;
            }
        }

        /// <summary>Initializes a new instance of the <see cref="PartitionReference"/> class with a specified name and unique identifier.</summary>
        /// <param name="name">The name of the partition.</param>
        /// <param name="uniqueId">The unique identifier of the partition.</param>
        public PartitionReference(string? name, string? uniqueId)
            : base()
        {
            this.name = name;
            this.uniqueId = uniqueId;
        }

        /// <summary>Gets the name of the partition reference.</summary>
        [JsonIgnore]
        public string? Name
        {
            get
            {
                return name;
            }
        }

        /// <summary>Gets the unique identifier for this reference.</summary>
        [JsonIgnore]
        public string? UniqueId
        {
            get
            {
                return uniqueId;
            }
        }

        /// <summary>Creates a deep copy of the current partition reference.</summary>
        /// <returns>A new <see cref="ISerializableObject"/> instance that is a clone of the current object.</returns>
        public override ISerializableObject? Clone()
        {
            return new PartitionReference(this);
        }

        /// <summary>Determines whether the specified object is equal to the current partition reference.</summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns>True if the objects are equal; otherwise, false.</returns>
        public override bool Equals(object? obj)
        {
            return obj is PartitionReference reference &&
                   base.Equals(obj) &&
                   name == reference.name &&
                   uniqueId == reference.uniqueId;
        }

        /// <summary>Returns the hash code for the current partition reference.</summary>
        /// <returns>A 32-bit signed integer hash code.</returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        /// <summary>Returns a string representation of the partition reference, combining the name and unique identifier with a separator.</summary>
        /// <returns>A string representing the partition reference, or null if the name or unique identifier is empty.</returns>
        public override string? ToString()
        {
            if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(uniqueId))
            {
                return null;
            }

            return $"{name}{Constants.Reference.Separator}{uniqueId}";
        }
    }
}