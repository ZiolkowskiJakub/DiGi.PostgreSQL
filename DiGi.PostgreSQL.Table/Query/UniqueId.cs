using DiGi.Core.IO.Table.Interfaces;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
        public static string? UniqueId(this IColumn? column)
        {
            if (column?.Name is not string name)
            {
                return null;
            }

            return name.ToLower().Trim().Replace(" ", "_").Replace("[", "").Replace("]", "").Replace(".", "_").Replace(",", "_");
        }
    }
}