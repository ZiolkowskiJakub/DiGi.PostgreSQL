using NpgsqlTypes;
using System;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
        /// <summary>
        /// Attempts to convert a given object to a value corresponding to the specified NpgsqlDbType.
        /// </summary>
        /// <param name="object">The source object to be converted.</param>
        /// <param name="result">When this method returns, contains the converted value if successful; otherwise, null.</param>
        /// <param name="npgsqlDbType">The target PostgreSQL database type for conversion.</param>
        /// <returns>True if the conversion was successful; otherwise, false.</returns>
        public static bool TryConvert(this object? @object, out object? result, NpgsqlDbType npgsqlDbType)
        {
            switch (npgsqlDbType)
            {
                case NpgsqlTypes.NpgsqlDbType.Integer:
                    return Core.Query.TryConvert(@object, out result, typeof(int));

                case NpgsqlTypes.NpgsqlDbType.Smallint:
                    return Core.Query.TryConvert(@object, out result, typeof(short));

                case NpgsqlTypes.NpgsqlDbType.Bigint:
                    return Core.Query.TryConvert(@object, out result, typeof(long));

                case NpgsqlTypes.NpgsqlDbType.Real:
                    return Core.Query.TryConvert(@object, out result, typeof(float));

                case NpgsqlTypes.NpgsqlDbType.Double:
                    return Core.Query.TryConvert(@object, out result, typeof(double));

                case NpgsqlTypes.NpgsqlDbType.Numeric:
                    return Core.Query.TryConvert(@object, out result, typeof(decimal));

                case NpgsqlTypes.NpgsqlDbType.Text:
                    return Core.Query.TryConvert(@object, out result, typeof(string));

                case NpgsqlTypes.NpgsqlDbType.Char:
                    return Core.Query.TryConvert(@object, out result, typeof(string));

                case NpgsqlTypes.NpgsqlDbType.Boolean:
                    return Core.Query.TryConvert(@object, out result, typeof(bool));

                case NpgsqlTypes.NpgsqlDbType.TimestampTz:
                    return Core.Query.TryConvert(@object, out result, typeof(DateTime));

                case NpgsqlTypes.NpgsqlDbType.Date:
                    return Core.Query.TryConvert(@object, out result, typeof(DateOnly));

                case NpgsqlTypes.NpgsqlDbType.Time:
                    return Core.Query.TryConvert(@object, out result, typeof(TimeOnly));

                case NpgsqlTypes.NpgsqlDbType.Interval:
                    return Core.Query.TryConvert(@object, out result, typeof(TimeSpan));
            }

            result = null;
            return false;
        }
    }
}