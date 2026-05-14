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

            await using NpgsqlBatch npgsqlBatch = new (npgsqlConnection);

            foreach (UColumn column in columns)
            {
                if (column is null)
                {
                    continue;
                }

                NpgsqlBatchCommand npgsqlBatchCommand = new (commandText);

                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("table_name", NpgsqlDbType.Text) { Value = tableName });
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("unique_id", NpgsqlDbType.Text) { Value = column.UniqueId() ?? string.Empty });
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("name", NpgsqlDbType.Text) { Value = column.Name });

                string? description = null;
                string? category = null;

                // Pattern matching for Extended properties
                if (column is ExtendedColumn extendedColumn)
                {
                    description = extendedColumn.Description;
                    category = extendedColumn.Category;
                }

                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("description", NpgsqlDbType.Text) { Value = (object?)description ?? DBNull.Value });
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("category", NpgsqlDbType.Text) { Value = (object?)category ?? DBNull.Value });
                npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("data", NpgsqlDbType.Jsonb) { Value = column.ToSystem_String() });

                npgsqlBatch.BatchCommands.Add(npgsqlBatchCommand);
            }

            try
            {
                // Execute all commands in a single round-trip
                int rowsAffected = await npgsqlBatch.ExecuteNonQueryAsync();
                return rowsAffected > 0;
            }
            catch (PostgresException ex)
            {
                // Log exception here (e.g., using Serilog or standard ILogger)
                System.Diagnostics.Debug.WriteLine($"{nameof(UpdateAsync)} error: {ex.Message}");
                return false;
            }
        }
    }
}