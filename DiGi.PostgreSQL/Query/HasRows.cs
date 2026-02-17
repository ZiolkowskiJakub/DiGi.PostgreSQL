using Npgsql;

namespace DiGi.PostgreSQL
{
    public static partial class Query
    {
        public static bool HasRows(this NpgsqlConnection? npgsqlConnection, string tableName)
        {
            // Basic validation for Revit/Rhino plugin stability
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            // Using 'SELECT 1' with 'LIMIT 1' is the most performant way to check for existence
            // We use string interpolation ONLY for the table name because it cannot be parameterized 
            // in standard SQL, but we should ensure the tableName is sanitized or trusted.
            string sql = $"SELECT EXISTS (SELECT 1 FROM public.{tableName} LIMIT 1);";

            using NpgsqlCommand npgsqlCommand = new NpgsqlCommand(sql, npgsqlConnection);

            try
            {
                object? result = npgsqlCommand.ExecuteScalar();

                if (result is bool exists)
                {
                    return exists;
                }

                return false;
            }
            catch (PostgresException ex) when (ex.SqlState == "42P01") // undefined_table
            {
                // Even if we check TableExists() beforehand, a race condition could occur 
                // in multi-user environments (e.g., another Revit user drops the table).
                return false;
            }
        }
    }
}