using System.ComponentModel;

namespace DiGi.PostgreSQL.Enums
{
    /// <summary>Defines the supported data types within the DiGi.Core system.</summary>
    [Description("Data Type")]
    public enum DataType
    {
        /// <summary>Undefined</summary>
        [Description("Undefined")] Undefined,
        /// <summary>The JSON data type.</summary>
        [Description("Json")] Json,
        /// <summary>The binary data type.</summary>
        [Description("Binary")] Binary,
        /// <summary>The archive data type.</summary>
        [Description("Archive")] Archive,
    }
}