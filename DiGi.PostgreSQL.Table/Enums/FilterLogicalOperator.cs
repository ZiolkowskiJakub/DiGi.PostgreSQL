namespace DiGi.PostgreSQL.Table.Enums
{
    /// <summary>
    /// Specifies the logical operator to combine multiple filter conditions or groups.
    /// </summary>
    public enum FilterLogicalOperator
    {
        /// <summary>
        /// Combines conditions or groups using logical AND.
        /// </summary>
        And,

        /// <summary>
        /// Combines conditions or groups using logical OR.
        /// </summary>
        Or
    }
}