using DiGi.Core.Classes;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Table.Classes
{
    /// <summary>
    /// Represents a PostgreSQL table structure and its data.
    /// </summary>
    public class Table : SerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Table"/> class using the provided <see cref="JsonObject"/>.
        /// </summary>
        /// <param name="jsonObject">The <see cref="JsonObject"/> containing the table data, or null to initialize an empty table.</param>
        public Table(JsonObject? jsonObject)

            : base(jsonObject)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Table"/> class by cloning an existing table instance.
        /// </summary>
        /// <param name="table">The source <see cref="Table"/> instance to clone from, or null to initialize an empty table.</param>
        public Table(Table? table)
            : base(table)
        {
            if (table is not null)
            {
                Columns = Core.Query.Clone(table.Columns) ?? [];
                if (table.Rows != null)
                {
                    foreach (object?[] row in table.Rows)
                    {
                        Rows.Add([.. row]);
                    }
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the Table class.
        /// </summary>
        public Table()
        {
        }

        /// <summary>
        /// Gets or sets the list of columns in the table.
        /// </summary>
        /// <example>
        /// [
        ///   { "Name": "Reference", "UniqueId": "reference", "Category": "Administrative" },
        ///   { "Name": "County Id", "UniqueId": "count_id", "Category": "Administrative" },
        ///   { "Name": "Floor area", "UniqueId": "floor_area", "Category": "Shape descriptors" }
        /// ]
        /// </example>
        [JsonInclude, JsonPropertyName(nameof(Columns))]
        public List<Column?> Columns { get; set; } = [];

        /// <summary>
        /// Gets or sets the data values in rows stored in the table.
        /// </summary>
        /// <example>
        /// [
        ///   [ "a71b3f91-819f-489a-93c3-a850948c60af", 10365, 308.38 ],
        ///   [ "b82c4g02-920g-590b-04d4-b961059d71bg", 10366, 309.49 ]
        /// ]
        /// </example>
        [JsonInclude, JsonPropertyName(nameof(Rows))]
        public List<object?[]> Rows { get; set; } = [];
    }
}
