namespace DiGi.PostgreSQL.Table.Constants
{
    /// <summary>
    /// Provides constant values for column names the table converter writes into its own statements.
    /// </summary>
    public static class ColumnName
    {
        /// <summary>
        /// The alias of the extra column carrying a row's physical position (<c>ctid</c> as text) in a physical-order read. It is read by the converter and never added to the pulled table.
        /// </summary>
        public const string PhysicalPosition = "__digi_physical_position";
    }
}
