using System.ComponentModel;

namespace DiGi.PostgreSQL.Enums
{
    [Description("PostgreSQL Data Type")]
    public enum PostgreSQLDataType
    {
        [Description("Undefined")] Undefined,
        [Description("Integer")] Integer,
        [Description("Bigint")] Bigint,
        [Description("Boolean")] Boolean,
        [Description("Character varying")] CharacterVarying,
        [Description("Text")] Text,
        [Description("Timestamp without time zone")] TimestampWithoutTimeZone,
        [Description("Timestamp with time zone")] TimestampWithTimeZone,
        [Description("Numeric")] Numeric,
        [Description("Uuid")] Uuid,
        [Description("Jsonb")] Jsonb,
        [Description("Bytea")] Bytea,
        [Description("Other")] Other,
    }
}