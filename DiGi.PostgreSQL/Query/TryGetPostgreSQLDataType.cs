using DiGi.PostgreSQL.Enums;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        /// <summary>
        /// Attempts to get the corresponding PostgreSQL data type from a given string representation.
        /// </summary>
        /// <param name="value">The string value representing the data type.</param>
        /// <param name="postgreSQLDataType">When this method returns, contains the parsed PostgreSQL data type if successful; otherwise, PostgreSQLDataType.Undefined.</param>
        /// <returns>True if the string was successfully converted to a PostgreSQL data type; otherwise, false.</returns>
        public static bool TryGetPostgreSQLDataType(this string value, out PostgreSQLDataType postgreSQLDataType)
        {
            postgreSQLDataType = PostgreSQLDataType.Undefined;

            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            string value_Temp = value.ToLower();

            switch (value_Temp)
            {
                case "integer":
                    postgreSQLDataType = PostgreSQLDataType.Integer;
                    break;

                case "bigint":
                    postgreSQLDataType = PostgreSQLDataType.Bigint;
                    break;

                case "boolean":
                    postgreSQLDataType = PostgreSQLDataType.Boolean;
                    break;

                default:
                    return false;
            }

            return true;
        }
    }
}