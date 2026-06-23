using DiGi.PostgreSQL.Table.Classes;
using System.Collections.Generic;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Modify
    {
        /// <summary>
        /// Recursively traverses a <see cref="FilterGroup"/> to collect all unique column identifiers.
        /// </summary>
        /// <param name="filterGroup">The filter group instance to traverse.</param>
        /// <param name="uniqueIds">The set to accumulate unique column identifiers in.</param>
        public static void CollectColumnUniqueIds(this FilterGroup? filterGroup, HashSet<string> uniqueIds)
        {
            if (filterGroup is null)
            {
                return;
            }

            if (filterGroup.FilterConditions is not null)
            {
                foreach (FilterCondition filterCondition in filterGroup.FilterConditions)
                {
                    if (filterCondition is not null && !string.IsNullOrWhiteSpace(filterCondition.ColumnUniqueId))
                    {
                        uniqueIds.Add(filterCondition.ColumnUniqueId);
                    }
                }
            }

            if (filterGroup.FilterGroups is not null)
            {
                foreach (FilterGroup filterGroup_Child in filterGroup.FilterGroups)
                {
                    if (filterGroup_Child is not null)
                    {
                        filterGroup_Child.CollectColumnUniqueIds(uniqueIds);
                    }
                }
            }
        }
    }
}