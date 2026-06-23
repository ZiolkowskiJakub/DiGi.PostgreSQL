using DiGi.Core.Classes;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Table.Classes
{
    /// <summary>
    /// Represents a column in a PostgreSQL table.
    /// </summary>
    public class Column : SerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Column"/> class from a <see cref="JsonObject"/>.
        /// </summary>
        /// <param name="jsonObject">The <see cref="JsonObject"/> used to initialize the column properties.</param>
        public Column(JsonObject? jsonObject)
            : base(jsonObject)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Column"/> class by cloning another Column instance.
        /// </summary>
        /// <param name="column">The source <see cref="Column"/> instance to copy values from.</param>
        public Column(Column? column)
            : base(column)
        {
            if (column is not null)
            {
                Category = column.Category;
                Description = column.Description;
                Index = column.Index;
                Name = column.Name;
                UniqueId = column.UniqueId;
            }
        }

        /// <summary>
        /// Initializes a new instance of the Column class.
        /// </summary>
        public Column()
        {
        }

        /// <summary>
        /// Gets or sets the category of the column.
        /// </summary>
        [JsonInclude, JsonPropertyName("Category")]
        public string? Category { get; set; }

        /// <summary>
        /// Gets or sets the description of the column reference.
        /// </summary>
        [JsonInclude, JsonPropertyName("Description")]
        public string? Description { get; set; }

        /// <summary>
        /// Gets or sets the unique identifier of the column.
        /// </summary>
        [JsonInclude, JsonPropertyName("Index")]
        public int Index { get; set; }

        /// <summary>
        /// Gets or sets the name of the column.
        /// </summary>
        [JsonInclude, JsonPropertyName("Name")]
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the unique identifier for the data column across different contexts.
        /// </summary>
        [JsonInclude, JsonPropertyName("UniqueId")]
        public string? UniqueId { get; set; }

        /// <summary>
        /// Gets or sets the values DataType for the column across different contexts.
        /// </summary>
        [JsonInclude, JsonPropertyName("DataType")]
        public Core.Enums.DataType? DataType { get; set; }
    }
}