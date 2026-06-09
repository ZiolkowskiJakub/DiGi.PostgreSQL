namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
        /// <summary>
        /// Converts the provided value into a formatted partition name suffix by converting it to lowercase, trimming whitespace, and replacing spaces with underscores.
        /// </summary>
        /// <param name="value">The object value to be processed.</param>
        /// <returns>A formatted string representing the partition name suffix, or null if the input is not a valid string.</returns>
        public static string? PartitionNameSuffix(object? value)
        {
            if (value?.ToString() is not string text)
            {
                return null;
            }

            return text.ToLower().Trim().Replace(" ", "_");
        }
    }
}