using DiGi.Core.IO.Table.Interfaces;
using System.Collections.Generic;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Convert
    {
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