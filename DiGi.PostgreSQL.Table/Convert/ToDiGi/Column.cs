using DiGi.Core.IO.Table.Classes;
using DiGi.Core.IO.Table.Interfaces;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Convert
    {
        public static Classes.Column? ToDiGi<UColumn>(this UColumn? column) where UColumn : IColumn
        {
            if (column is null)
            {
                return null;
            }

            Classes.Column result = new()
            {
                Index = column.Index,
                Name = column.Name,
                DataType = Core.Query.DataType(column.Type)
            };

            if (column is ExtendedColumn extendedColumn)
            {
                result.Category = extendedColumn.Category;
                result.Description = extendedColumn.Description;
            }

            return result;
        }
    }
}