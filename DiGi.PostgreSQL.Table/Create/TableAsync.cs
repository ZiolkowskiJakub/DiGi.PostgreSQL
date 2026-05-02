using DiGi.Core.IO.Table.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.Table.Classes;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Create
    {
        public static async Task<bool> TableAsync<UColumn>(this NpgsqlConnection? npgsqlConnection, string tableName, TableConversionOptions<UColumn>? tableConversionOptions, IEnumerable<UColumn> columns) where UColumn : IColumn
        {
            if(string.IsNullOrWhiteSpace(tableName) || npgsqlConnection is null)
            {
                return false;
            }

            Dictionary<string, UColumn> dictionary = [];
            if (tableConversionOptions is not null)
            {
                if (tableConversionOptions.PrimaryKeyColumns is List<UColumn> columns_TableConversionOptions_PrimaryKey)
                {
                    foreach (UColumn column in columns_TableConversionOptions_PrimaryKey)
                    {
                        if (!string.IsNullOrWhiteSpace(column?.Name))
                        {
                            dictionary[column.Name] = column;
                        }
                    }
                }

                if (tableConversionOptions.PartitioningOptions is PartitioningOptions<UColumn> partitioningOptions)
                {
                    if (partitioningOptions.Column?.Name is string name && !string.IsNullOrWhiteSpace(name))
                    {
                        dictionary[name] = partitioningOptions.Column;
                    }
                }
            }

            List<UColumn> columns_PrimaryKey = [.. dictionary.Values];
            List<UColumn> columns_NotPrimaryKey = [];

            if (columns is not null)
            {
                foreach (UColumn column in columns)
                {
                    if (column is not null && !string.IsNullOrWhiteSpace(column.Name) && !dictionary.ContainsKey(column.Name))
                    {
                        dictionary[column.Name] = column;
                        columns_NotPrimaryKey.Add(column);
                    }
                }
            }

            List<UColumn> columns_All = [.. dictionary.Values];

            List<string>? columnNames = await Query.ColumnNamesAsync(npgsqlConnection, tableName);
            if (columnNames != null && columnNames.Count >= columns_All.Count)
            {
                bool update = false;

                for (int i = columns_All.Count - 1; i >= 0; i--)
                {
                    if (!columnNames.Contains(columns_All[i].Name!))
                    {
                        update = true;
                        break;
                    }
                }

                if(!update)
                {
                    return true;
                }
            }

            columns_PrimaryKey.Sort((x, y) => x.Index.CompareTo(y.Index));
            foreach(UColumn column in columns_PrimaryKey)
            {
                if(columnNames is not null && columnNames.Contains(column.Name!))
                {
                    continue;
                }
            }

            columns_NotPrimaryKey.Sort((x, y) => x.Index.CompareTo(y.Index));
            foreach (UColumn column in columns_NotPrimaryKey)
            {
                if (columnNames is not null && columnNames.Contains(column.Name!))
                {
                    continue;
                }
            }

            throw new System.NotImplementedException();

        }


    }

}
