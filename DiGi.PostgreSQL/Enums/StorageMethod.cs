using System.ComponentModel;

namespace DiGi.PostgreSQL.Enums
{
    /// <summary>
    /// Specifies the method used for data storage within the PostgreSQL database.
    /// </summary>
    [Description("StorageMethod")]
    public enum StorageMethod
    {
        /// <summary>
        /// Indicates that the storage method is undefined.
        /// </summary>
        [Description("Undefined")] Undefined,

        /// <summary>
        /// Indicates storage using a unique reference identifier.
        /// </summary>
        [Description("UniqueReference")] UniqueReference,

        /// <summary>
        /// Indicates storage using a standard table structure.
        /// </summary>
        [Description("Table")] Table,

        /// <summary>
        /// Indicates storage using a partition reference.
        /// </summary>
        [Description("PartitionReference")] PartitionReference,

        /// <summary>
        /// Indicates storage using a unique reference within a specific partition.
        /// </summary>
        [Description("PartitionUniqueReference")] PartitionUniqueReference,
    }
}