using DiGi.Core;
using DiGi.Core.IO.Table.Classes;
using DiGi.Core.IO.Table.Interfaces;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Modify
    {
        public static async Task<bool> UpdateAsync<UColumn>(this NpgsqlConnection? npgsqlConnection, string tableName, IEnumerable<UColumn> columns) where UColumn : IColumn
        {
            if (npgsqlConnection is null || columns is null || string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            const string commandText = $@"
                INSERT INTO {Constants.TableName.Columns} (table_name, unique_id, name, description, category, unit, data)
                VALUES (@table_name, @unique_id, @name, @description, @category, @unit, @data::jsonb)
                ON CONFLICT (table_name, unique_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category,
                    unit = EXCLUDED.unit,
                    data = EXCLUDED.data;";

            await using NpgsqlBatch npgsqlBatch = new(npgsqlConnection);

            foreach (UColumn column in columns)
            {
                if (column is null)
                {
                    continue;
                }

                NpgsqlBatchCommand npgsqlBatchCommand = new(commandText);
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("table_name", NpgsqlDbType.Text) { Value = tableName });
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("unique_id", NpgsqlDbType.Text) { Value = column.UniqueId() ?? string.Empty });
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("name", NpgsqlDbType.Text) { Value = column.Name });

                if (column is ExtendedColumn extendedColumn)
                {
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("description", NpgsqlDbType.Text) { Value = extendedColumn.Description });
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("category", NpgsqlDbType.Text) { Value = extendedColumn.Category });
                    //npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("unit", NpgsqlDbType.Text) { Value = extendedColumn.Unit });
                }
                else
                {
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("description", NpgsqlDbType.Text) { Value = DBNull.Value });
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("category", NpgsqlDbType.Text) { Value = DBNull.Value });
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("unit", NpgsqlDbType.Text) { Value = DBNull.Value });
                }

                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("data", NpgsqlDbType.Jsonb) { Value = column.ToSystem_String() });
            }

            throw new System.NotImplementedException();
        }
    }
}