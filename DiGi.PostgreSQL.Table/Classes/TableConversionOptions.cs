using DiGi.Core.IO.Table.Interfaces;
using System.Collections.Generic;

namespace DiGi.PostgreSQL.Table.Classes
{
    /// <summary>
    /// Provides options for converting a table to PostgreSQL format.
    /// </summary>
    /// <typeparam name="UColumn">The type of column used in the table conversion, which must implement IColumn.</typeparam>
    public class TableConversionOptions<UColumn> where UColumn : IColumn
    {
        /// <summary>
        /// Gets or sets the list of columns that serve as the primary key for the table.
        /// </summary>
        public List<UColumn>? PrimaryKeyColumns { get; set; }

        /// <summary>
        /// Gets or sets the list of columns that are defined with unique constraints.
        /// </summary>
        public List<UColumn>? UniqueColumns { get; set; }

        /// <summary>
        /// Gets or sets the partitioning options for the table.
        /// </summary>
        public PartitioningOptions<UColumn>? PartitioningOptions { get; set; }

        /// <summary>
        /// Gets or sets the column designated as the identity column.
        /// </summary>
        public UColumn? IdentityColumn { get; set; }
    }
}