using System.ComponentModel;

namespace DiGi.PostgreSQL.Enums
{
    /// <summary>
    /// Represents the PostgreSQL data types.
    /// </summary>
    [Description("PostgreSQL Data Type")]
    public enum PostgreSQLDataType
    {
        /// <summary>
        /// Undefined data type.
        /// </summary>
        [Description("Undefined")] Undefined,
        /// <summary>
        /// Integer data type.
        /// </summary>
        [Description("Integer")] Integer,
        /// <summary>
        /// Bigint data type.
        /// </summary>
        [Description("Bigint")] Bigint,
        /// <summary>
        /// Boolean data type.
        /// </summary>
        [Description("Boolean")] Boolean,
        /// <summary>
        /// Character varying data type.
        /// </summary>
        [Description("Character varying")] CharacterVarying,
        /// <summary>
        /// Text data type.
        /// </summary>
        [Description("Text")] Text,
        /// <summary>
        /// Timestamp without time zone data type.
        /// </summary>
        [Description("Timestamp without time zone")] TimestampWithoutTimeZone,
        /// <summary>
        /// Timestamp with time zone data type.
        /// </summary>
        [Description("Timestamp with time zone")] TimestampWithTimeZone,
        /// <summary>
        /// Numeric data type.
        /// </summary>
        [Description("Numeric")] Numeric,
        /// <summary>
        /// Uuid data type.
        /// </summary>
        [Description("Uuid")] Uuid,
        /// <summary>
        /// Jsonb data type.
        /// </summary>
        [Description("Jsonb")] Jsonb,
        /// <summary>
        /// Bytea data type.
        /// </summary>
        [Description("Bytea")] Bytea,
        /// <summary>
        /// Other data type.
        /// </summary>
        [Description("Other")] Other,
    }
}