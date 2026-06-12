using System.ComponentModel;

namespace DiGi.PostgreSQL.Table.Enums
{
    /// <summary>
    /// Specifies statistical and text-parsing aggregation calculations for multi-value column operations.
    /// </summary>
    [Description("Specifies statistical and text-parsing aggregation calculations for multi-value column operations.")]
    public enum MultivalueAggregateFunction
    {
        /// <summary>Splits multi-value string items by a separator, and counts unique sub-items.</summary>
        [Description("Splits multi-value string items by a separator, and counts unique sub-items.")]
        SplitDistinctCount,

        /// <summary>Splits multi-value string items by a separator, groups them, and counts sub-item frequencies.</summary>
        [Description("Splits multi-value string items by a separator, groups them, and counts sub-item frequencies.")]
        SplitValueDistribution
    }
}
