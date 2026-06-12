using System.ComponentModel;

namespace DiGi.PostgreSQL.Table.Enums
{
    /// <summary>
    /// Specifies statistical aggregation calculations for single-value column operations.
    /// </summary>
    [Description("Specifies statistical aggregation calculations for single-value column operations.")]
    public enum SinglevalueAggregateFunction
    {
        /// <summary>Calculates the average value of a column.</summary>
        [Description("Calculates the average value of a column.")]
        Avg,

        /// <summary>Calculates the sum total of a column.</summary>
        [Description("Calculates the sum total of a column.")]
        Sum,

        /// <summary>Finds the minimum value in a column.</summary>
        [Description("Finds the minimum value in a column.")]
        Min,

        /// <summary>Finds the maximum value in a column.</summary>
        [Description("Finds the maximum value in a column.")]
        Max,

        /// <summary>Counts the number of non-null records in a column.</summary>
        [Description("Counts the number of non-null records in a column.")]
        Count,

        /// <summary>Counts the unique values in a column.</summary>
        [Description("Counts the unique values in a column.")]
        DistinctCount
    }
}
