using DiGi.Core.IO.Table.Classes;
using DiGi.Core.IO.Table.Interfaces;
using System.Collections.Generic;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Convert
    {
        /// <summary>
        /// Converts a collection of column implementations to DiGi core column representations.
        /// <para>Columns that convert to nothing are left out rather than carried through as nulls, so the result is always safe to enumerate.</para>
        /// </summary>
        /// <typeparam name="UColumn">The type of the columns being converted, which must implement <see cref="IColumn"/>.</typeparam>
        /// <param name="columns">The source columns to convert.</param>
        /// <returns>A list of <see cref="Classes.Column"/> instances if the input is not null; otherwise, null.</returns>
        public static List<Classes.Column>? ToDiGi<UColumn>(this IEnumerable<UColumn>? columns) where UColumn : IColumn
        {
            if (columns is null)
            {
                return null;
            }

            List<Classes.Column> result = [];
            foreach (UColumn column in columns)
            {
                if (ToDiGi(column) is Classes.Column column_Result)
                {
                    result.Add(column_Result);
                }
            }

            return result;
        }

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
                DataType = Core.Query.DataType(column.Type),
                UniqueId = column.UniqueId()
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