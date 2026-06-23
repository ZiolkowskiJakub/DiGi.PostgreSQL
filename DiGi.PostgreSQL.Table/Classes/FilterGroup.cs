using DiGi.PostgreSQL.Table.Enums;
using System.Collections.Generic;

namespace DiGi.PostgreSQL.Table.Classes
{
    /// <summary>
    /// Represents a group of filter conditions and sub-groups combined by a logical operator.
    /// </summary>
    public class FilterGroup
    {
        /// <summary>
        /// Gets or sets the logical operator (AND or OR) used to combine the elements within this group.
        /// </summary>
        public FilterLogicalOperator LogicalOperator { get; set; } = FilterLogicalOperator.And;

        /// <summary>
        /// Gets or sets the list of individual filter conditions within this group.
        /// </summary>
        public List<FilterCondition> FilterConditions { get; set; } = [];

        /// <summary>
        /// Gets or sets the list of sub-groups nested under this group.
        /// </summary>
        public List<FilterGroup> FilterGroups { get; set; } = [];
    }
}