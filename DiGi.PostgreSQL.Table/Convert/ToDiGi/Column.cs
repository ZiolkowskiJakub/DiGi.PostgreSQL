using DiGi.Core.IO.Table.Classes;
using DiGi.Core.IO.Table.Interfaces;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Convert
    {
        /// <summary>
        /// Converts a column implementation to a DiGi core column representation.
        /// </summary>
        /// <typeparam name="UColumn">The type of the column being converted, which must implement <see cref="IColumn"/>.</typeparam>
        /// <param name="column">The source column instance to convert.</param>
        /// <returns>A <see cref="Classes.Column"/> instance if the input is not null; otherwise, null.</returns>
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