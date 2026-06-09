using DiGi.Core.IO.Table.Interfaces;
using System.Collections.Generic;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Convert
    {
        /// <summary>
        /// Converts a generic table implementation to a standard <see cref="Classes.Table"/> object.
        /// </summary>
        /// <typeparam name="UTable">The type of the table implementing <see cref="ITable{TColumn, TRow}"/>.</typeparam>
        /// <typeparam name="UColumn">The type of the column implementing <see cref="IColumn"/>.</typeparam>
        /// <typeparam name="URow">The type of the row implementing <see cref="IRow{TRow}"/>.</typeparam>
        /// <param name="table">The source table to convert.</param>
        /// <returns>A converted <see cref="Classes.Table"/> instance, or null if the input table is null.</returns>
        public static Classes.Table? ToDiGi<UTable, UColumn, URow>(this UTable? table) where UTable : ITable<UColumn, URow>, new() where UColumn : IColumn where URow : IRow<URow>
        {
            if (table is null)
            {
                return null;
            }

            Classes.Table? result = new();

            if (table.Columns is not IEnumerable<UColumn> columns)
            {
                return result;
            }

            foreach (UColumn column in columns)
            {
                result.Columns.Add(column?.ToDiGi());
            }

            if (table.Rows is not IEnumerable<URow> rows)
            {
                return result;
            }

            foreach (URow row in rows)
            {
                result.Values.Add(row?.GetValues() ?? []);
            }

            return result;
        }
    }
}