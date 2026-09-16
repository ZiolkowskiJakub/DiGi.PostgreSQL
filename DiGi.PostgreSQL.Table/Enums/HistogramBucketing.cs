using System.ComponentModel;

namespace DiGi.PostgreSQL.Table.Enums
{
    /// <summary>
    /// Specifies how a value distribution histogram divides a column's values into buckets.
    /// </summary>
    [Description("Specifies how a value distribution histogram divides a column's values into buckets.")]
    public enum HistogramBucketing
    {
        /// <summary>Buckets of equal value width over the scope's [min, max] (width_bucket); the maximum lands in the overflow bucket bucketCount + 1.</summary>
        [Description("Buckets of equal value width over the scope's [min, max] (width_bucket); the maximum lands in the overflow bucket bucketCount + 1.")]
        EqualWidth = 0,

        /// <summary>Buckets of equal row count in value order (ntile); every bucket holds floor or ceiling of rows / bucketCount rows, ties may span buckets, and there is no overflow bucket.</summary>
        [Description("Buckets of equal row count in value order (ntile); every bucket holds floor or ceiling of rows / bucketCount rows, ties may span buckets, and there is no overflow bucket.")]
        EqualCount = 1
    }
}
