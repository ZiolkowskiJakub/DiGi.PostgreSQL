using DiGi.Core;
using DiGi.Core.IO.Table.Classes;
using DiGi.Core.IO.Table.Interfaces;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Modify
    {
        /// <summary>
        /// Updates or inserts column definitions into the PostgreSQL database for a specified table using an upsert operation.
        /// </summary>
        /// <typeparam name="UColumn">The type of the column being updated, which must implement <see cref="IColumn"/>.</typeparam>
        /// <param name="npgsqlConnection">The <see cref="NpgsqlConnection"/> instance used to communicate with the PostgreSQL database.</param>
        /// <param name="tableName">The name of the table whose columns are being updated.</param>
        /// <param name="columns">A collection of column objects implementing <see cref="IColumn"/> to be updated or inserted.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is <c>true</c> if one or more rows were affected; otherwise, <c>false</c>.</returns>
        public static async Task<bool> UpdateAsync<UColumn>(this NpgsqlConnection? npgsqlConnection, string tableName, IEnumerable<UColumn> columns) where UColumn : IColumn
        {
            if (npgsqlConnection is null || columns is null || string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            // Ensure connection is open if we are managing it here,
            // though usually, it's better to expect an open connection in an extension method.
            if (npgsqlConnection.State != System.Data.ConnectionState.Open)
            {
                await npgsqlConnection.OpenAsync();
            }

            const string commandText = $@"
                INSERT INTO {Constants.TableName.Columns} (table_name, unique_id, name, description, category, data)
                VALUES (@table_name, @unique_id, @name, @description, @category, @data::jsonb)
                ON CONFLICT (table_name, unique_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category,
                    data = EXCLUDED.data;";

            await using NpgsqlBatch npgsqlBatch = new(npgsqlConnection);

            foreach (UColumn column in columns)
            {
                if (column is null)
                {
                    continue;
                }

                NpgsqlBatchCommand npgsqlBatchCommand = new(commandText);

                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("table_name", NpgsqlTypes.NpgsqlDbType.Text) { Value = tableName });
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("unique_id", NpgsqlTypes.NpgsqlDbType.Text) { Value = column.UniqueId() ?? string.Empty });
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("name", NpgsqlTypes.NpgsqlDbType.Text) { Value = column.Name });

                string? description = null;
                string? category = null;

                // Pattern matching for Extended properties
                if (column is ExtendedColumn extendedColumn)
                {
                    description = extendedColumn.Description;
                    category = extendedColumn.Category;
                }

                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("description", NpgsqlTypes.NpgsqlDbType.Text) { Value = (object?)description ?? DBNull.Value });
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("category", NpgsqlTypes.NpgsqlDbType.Text) { Value = (object?)category ?? DBNull.Value });
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("data", NpgsqlTypes.NpgsqlDbType.Jsonb) { Value = column.ToSystem_String() });

                npgsqlBatch.BatchCommands.Add(npgsqlBatchCommand);
            }

            int rowsAffected = 0;

            try
            {
                rowsAffected = await npgsqlBatch.ExecuteNonQueryAsync();
            }
            catch (Exception)
            {
                rowsAffected = -1;
            }

            return rowsAffected > 0;
        }
    }
}