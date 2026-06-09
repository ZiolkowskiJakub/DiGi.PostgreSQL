using DiGi.Core.Classes;
using DiGi.PostgreSQL.Enums;
using DiGi.PostgreSQL.Interfaces;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Classes
{
    /// <summary>
    /// Represents a partition within the PostgreSQL database context.
    /// </summary>
    public class Partition : SerializableObject, IPostgreSQLSerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Partition"/> class using a JSON object.
        /// </summary>
        /// <param name="jsonObject">The JSON object used to initialize the partition.</param>
        public Partition(JsonObject jsonObject)
            : base(jsonObject)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Partition"/> class by copying an existing partition.
        /// </summary>
        /// <param name="partition">The source partition to copy from.</param>
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

        /// <summary>
        /// Initializes a new instance of the <see cref="Partition"/> class with specified values.
        /// </summary>
        /// <param name="id">The unique identifier for the partition.</param>
        /// <param name="name">The name of the partition.</param>
        /// <param name="dataType">The data type associated with the partition.</param>
        public Partition(short id, string name, DataType dataType)
        {
            Id = id;
            Name = name;
            DataType = dataType;
        }

        /// <summary>
        /// Gets the data type of the partition.
        /// </summary>
        [JsonInclude, JsonPropertyName("DataType")]
        public DataType DataType { get; }

        /// <summary>
        /// Gets the unique identifier of the partition.
        /// </summary>
        [JsonInclude, JsonPropertyName("Id")]
        public short Id { get; }

        /// <summary>
        /// Gets the name of the partition.
        /// </summary>
        [JsonInclude, JsonPropertyName("Name")]
        public string? Name { get; }
    }
}