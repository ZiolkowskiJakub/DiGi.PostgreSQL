using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.PartitionReference.Classes
{
    /// <summary>Represents a reference to a partition.</summary>
    /// <example>
    /// Renders and parses (via <see cref="Core.Query.TryParse(string?, out IReference?)"/>) as the discriminator, the
    /// partition name, then the unique identifier:
    /// <code>Partition::building2d::0f8fad5bd9cb469fa16570867728950e</code>
    /// </example>
    /// <remarks>
    /// TODO [ReferenceFormat]: The rendered form changed. It used to be <c>name-&gt;uniqueId</c>, where <c>-&gt;</c>
    /// came from a local Constants.Reference that shadowed DiGi.Core's; it is now the shared discriminated grammar,
    /// and it no longer returns null when a field is blank (which made every blank instance compare equal).
    /// This is database-safe: a partition is named from the <see cref="Name"/> property - see
    /// Modify/RemoveAsync.cs, which groups by <c>x =&gt; x?.Name</c> - not from this string, which only feeds
    /// equality. No partition or table migration is required.
    /// </remarks>
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

        /// <summary>Gets the segments of this reference's string form: the partition name, then the unique identifier.</summary>
        [JsonIgnore]
        protected override IEnumerable<string?> Segments
        {
            get
            {
                return [Core.Query.Segment(name), Core.Query.Segment(uniqueId)];
            }
        }
    }
}