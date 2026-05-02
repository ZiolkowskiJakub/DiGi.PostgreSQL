using DiGi.Core.IO.Table.Interfaces;

namespace DiGi.PostgreSQL.Table.Classes
{
    public class PartitioningOptions<UColumn> where UColumn : IColumn
    {
        public UColumn? Column { get; set; }

        public string? DefaultSuffix { get; set; }

        public PartitioningRule? PartitioningRule { get; set; }

    }
}
