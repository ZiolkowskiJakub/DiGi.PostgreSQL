using DiGi.Core.IO.Table.Interfaces;

namespace DiGi.PostgreSQL.Table.Classes
{
    /// <summary>
    /// Options for configuring the partitioning of a table.
    /// </summary>
    /// <typeparam name="UColumn">The type of the column used for partitioning, which must implement <see cref="IColumn"/>.</typeparam>
    public class PartitioningOptions<UColumn> where UColumn : IColumn
    {
        /// <summary>
        /// Gets or sets the column used as the partition key.
        /// </summary>
        public UColumn? Column { get; set; }

        /// <summary>
        /// Gets or sets the default suffix applied to partition table names.
        /// </summary>
        public string? DefaultSuffix { get; set; }

        /// <summary>
        /// Gets or sets the rule used for partitioning the table.
        /// </summary>
        public PartitioningRule? PartitioningRule { get; set; }
    }
}