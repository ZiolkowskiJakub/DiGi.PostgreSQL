namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
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