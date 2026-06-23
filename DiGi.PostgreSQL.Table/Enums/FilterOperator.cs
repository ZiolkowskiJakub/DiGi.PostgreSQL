namespace DiGi.PostgreSQL.Table.Enums
{
    /// <summary>
    /// Specifies the comparison operator to be applied to a database column filter.
    /// </summary>
    public enum FilterOperator
    {
        /// <summary>
        /// Checks if the column value is equal to the filter value. Applicable to both text and numeric columns.
        /// </summary>
        Equals,

        /// <summary>
        /// Checks if the column value is not equal to the filter value. Applicable to both text and numeric columns.
        /// </summary>
        NotEquals,

        /// <summary>
        /// Checks if the column value is greater than the filter value. Applicable to numeric columns only.
        /// </summary>
        GreaterThan,

        /// <summary>
        /// Checks if the column value is greater than or equal to the filter value. Applicable to numeric columns only.
        /// </summary>
        GreaterThanOrEqual,

        /// <summary>
        /// Checks if the column value is less than the filter value. Applicable to numeric columns only.
        /// </summary>
        LessThan,

        /// <summary>
        /// Checks if the column value is less than or equal to the filter value. Applicable to numeric columns only.
        /// </summary>
        LessThanOrEqual,

        /// <summary>
        /// Checks if the column value matches any of the values in the specified collection parameter. Applicable to both text and numeric columns.
        /// </summary>
        In,

        /// <summary>
        /// Checks if the column value does not match any of the values in the specified collection parameter. Applicable to both text and numeric columns.
        /// </summary>
        NotIn,

        /// <summary>
        /// Checks if the column value contains the filter string value as a substring (case-insensitive search). Applicable to text and string columns only.
        /// </summary>
        Contains,

        /// <summary>
        /// Checks if the column value is null. Applicable to all column types.
        /// </summary>
        IsNull,

        /// <summary>
        /// Checks if the column value is not null. Applicable to all column types.
        /// </summary>
        IsNotNull
    }
}