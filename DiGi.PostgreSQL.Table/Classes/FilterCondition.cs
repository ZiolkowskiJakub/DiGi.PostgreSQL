using DiGi.PostgreSQL.Table.Enums;
using System.ComponentModel.DataAnnotations;

namespace DiGi.PostgreSQL.Table.Classes
{
    /// <summary>
    /// Represents a single comparison filter condition on a database column.
    /// </summary>
    public class FilterCondition
    {
        /// <summary>
        /// Gets or sets the unique identifier of the column to filter.
        /// </summary>
        [Required]
        public string? ColumnUniqueId { get; set; }

        /// <summary>
        /// Gets or sets the comparison operator to apply.
        /// </summary>
        [Required]
        public FilterOperator FilterOperator { get; set; }

        /// <summary>
        /// Gets or sets the value to compare against. For list operators like In or NotIn, this should be a collection of values.
        /// </summary>
        public object? Value { get; set; }
    }
}