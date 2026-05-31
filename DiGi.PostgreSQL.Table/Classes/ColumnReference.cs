using DiGi.Core.Classes;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Table.Classes
{
    /// <summary>
    /// Represents a reference to a column in a PostgreSQL table.
    /// </summary>
    public class ColumnReference : SerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnReference"/> class from a <see cref="JsonObject"/>.
        /// </summary>
        /// <param name="jsonObject">The JSON object to initialize from.</param>
        public ColumnReference(JsonObject? jsonObject)

            : base(jsonObject)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnReference"/> class by copying another <see cref="ColumnReference"/> instance.
        /// </summary>
        /// <param name="columnReference">The column reference to copy.</param>
        public ColumnReference(ColumnReference? columnReference)
            : base(columnReference)
        {
            if(columnReference is not null)
            {
                Category = columnReference.Category;
                Description = columnReference.Description;
                Id = columnReference.Id;
                Name = columnReference.Name;
                TableName = columnReference.TableName;
                UniqueId = columnReference.UniqueId;
            }
        }

        /// <summary>
        /// Initializes a new instance of the ColumnReference class.
        /// </summary>
        public ColumnReference()
        {
        }

        /// <summary>
        /// Gets or sets the category of the column reference.
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
        [JsonInclude, JsonPropertyName("Id")]
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets the name of the column.
        /// </summary>
        [JsonInclude, JsonPropertyName("Name")]
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the name of the table containing the column.
        /// </summary>
        [JsonInclude, JsonPropertyName("TableName")]
        public string? TableName { get; set; }

        /// <summary>
        /// Gets or sets the unique identifier for the column reference across different contexts.
        /// </summary>
        [JsonInclude, JsonPropertyName("UniqueId")]
        public string? UniqueId { get; set; }
    }
}
