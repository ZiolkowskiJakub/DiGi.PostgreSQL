using DiGi.PostgreSQL.Enums;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static bool TryGetPostgreSQLDataType(this string value, out PostgreSQLDataType postgreSQLDataType)
        {
            postgreSQLDataType = PostgreSQLDataType.Undefined;

            if(string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            string value_Temp = value.ToLower();

            switch(value_Temp)
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