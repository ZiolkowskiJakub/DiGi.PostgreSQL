using System.ComponentModel;

namespace DiGi.PostgreSQL.Enums
{
    [Description("Storage Method")]
    public enum StorageMethod
    {
        [Description("Undefined")] Undefined,
        [Description("UniqueReference")] UniqueReference,
        [Description("PartitionReference")] PartitionReference,
        [Description("PartitionUniqueReference")] PartitionUniqueReference,
    }
}