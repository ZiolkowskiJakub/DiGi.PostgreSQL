using DiGi.Core.IO.Table.Interfaces;
using System.Collections.Generic;

namespace DiGi.PostgreSQL.Table.Classes
{
    public class TableConversionOptions<UColumn> where UColumn : IColumn
    {
        public List<UColumn>? PrimaryKeyColumns { get; set; }

        public PartitioningOptions<UColumn>? PartitioningOptions { get; set; }
    }
}