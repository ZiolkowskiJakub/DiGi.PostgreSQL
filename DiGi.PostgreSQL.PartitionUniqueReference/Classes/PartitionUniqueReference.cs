using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.PartitionUniqueReference.Classes
{
    /// <summary>
    /// Represents a unique reference for a partition.
    /// </summary>
    /// <example>
    /// Renders and parses (via <see cref="Core.Query.TryParse(string?, out IReference?)"/>) as the discriminator, the
    /// partition name, then the nested unique reference:
    /// <code>PartitionUnique::building2d::(Guid::(Type::DiGi.GIS.Classes.Building2D,DiGi.GIS)::0f8fad5bd9cb469fa16570867728950e)</code>
    /// </example>
    /// <remarks>
    /// TODO [ReferenceFormat]: The rendered form changed. It used to be <c>name-&gt;nestedReference</c>, where
    /// <c>-&gt;</c> came from a local Constants.Reference that shadowed DiGi.Core's; it is now the shared
    /// discriminated grammar, and it no longer returns null when a field is missing (which made every blank instance
    /// compare equal). Database-safe for the same reason as <see cref="PartitionReference"/>: partitions are named
    /// from the Name property, not from this string.
    /// </remarks>
    public class PartitionUniqueReference : SerializableReference
    {
        [JsonInclude, JsonPropertyName("Name")]
        private readonly string? name;

        [JsonInclude, JsonPropertyName("UniqueReference")]
        private readonly IUniqueReference? uniqueReference;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionUniqueReference"/> class.
        /// </summary>
        public PartitionUniqueReference()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionUniqueReference"/> class from a JSON object.
        /// </summary>
        /// <param name="jsonObject">The JSON object to initialize from.</param>
        public PartitionUniqueReference(JsonObject? jsonObject)
            : base(jsonObject)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionUniqueReference"/> class by copying an existing instance.
        /// </summary>
        /// <param name="partitionUniqueReference">The source instance to copy.</param>
        public PartitionUniqueReference(PartitionUniqueReference? partitionUniqueReference)
            : base(partitionUniqueReference)
        {
            if (partitionUniqueReference is not null)
            {
                name = partitionUniqueReference.name;
                uniqueReference = partitionUniqueReference.uniqueReference;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionUniqueReference"/> class with the specified name and unique reference.
        /// </summary>
        /// <param name="name">The name of the partition.</param>
        /// <param name="uniqueReference">The unique reference for the partition.</param>
        public PartitionUniqueReference(string? name, IUniqueReference? uniqueReference)
            : base()
        {
            this.name = name;
            this.uniqueReference = uniqueReference;
        }

        /// <summary>
        /// Gets the name of the partition unique reference.
        /// </summary>
        [JsonIgnore]
        public string? Name
        {
            get
            {
                return name;
            }
        }

        /// <summary>
        /// Gets the unique reference associated with the partition.
        /// </summary>
        [JsonIgnore]
        public IUniqueReference? UniqueReference
        {
            get
            {
                return uniqueReference;
            }
        }

        /// <summary>
        /// Creates a deep copy of the current partition unique reference.
        /// </summary>
        /// <returns>A new <see cref="ISerializableObject"/> instance that is a clone of this object.</returns>
        public override ISerializableObject? Clone()
        {
            return new PartitionUniqueReference(this);
        }

        /// <summary>
        /// Determines whether the specified object is equal to the current partition unique reference.
        /// </summary>
        /// <param name="obj">The object to compare with the current instance.</param>
        /// <returns>True if the objects are equal; otherwise, false.</returns>
        public override bool Equals(object? obj)
        {
            return obj is PartitionUniqueReference partitionUniqueReference &&
                   base.Equals(obj) &&
                   name == partitionUniqueReference.name &&
                   uniqueReference?.ToString() == partitionUniqueReference.uniqueReference?.ToString();
        }

        /// <summary>
        /// Gets the hash code for the current partition unique reference.
        /// </summary>
        /// <returns>A 32-bit signed integer hash code.</returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        /// <summary>Gets the segments of this reference's string form: the partition name, then the nested unique reference.</summary>
        [JsonIgnore]
        protected override IEnumerable<string?> Segments
        {
            get
            {
                return [Core.Query.Segment(name), Core.Query.Segment(uniqueReference)];
            }
        }
    }
}