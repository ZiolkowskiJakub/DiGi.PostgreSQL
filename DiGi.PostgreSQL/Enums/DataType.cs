using System.ComponentModel;

namespace DiGi.PostgreSQL.Enums
{
    [Description("Data Type")]
    public enum DataType
    {
        [Description("Undefined")] Undefined,
        [Description("Json")] Json,
        [Description("Binary")] Binary,
        [Description("Archive")] Archive,
    }
}