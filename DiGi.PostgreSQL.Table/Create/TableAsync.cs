using DiGi.Core.IO.Table.Interfaces;
using DiGi.PostgreSQL.Table.Classes;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Create
    {
        public static async Task<bool> TableAsync<UColumn>(this NpgsqlConnection? npgsqlConnection, string tableName, TableConversionOptions<UColumn>? tableConversionOptions, IEnumerable<UColumn> columns) where UColumn : IColumn
        {
            if (string.IsNullOrWhiteSpace(tableName) || npgsqlConnection is null)
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

                if (!update)
                {
                    return true;
                }
            }

            columns_PrimaryKey.Sort((x, y) => x.Index.CompareTo(y.Index));
            foreach (UColumn column in columns_PrimaryKey)
            {
                if (columnNames is not null && columnNames.Contains(column.Name!))
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

            if (!await Query.TableExistsAsync(npgsqlConnection, tableName))
            {
                // Create data table
            }

            if (!await Query.TableExistsAsync(npgsqlConnection, Constants.TableName.Columns))
            {
                // Create columns table
            }

            await Modify.UpdateAsync(npgsqlConnection, tableName, columns_All);

            throw new System.NotImplementedException();
        }

        /// <summary>
        /// Initializes the metadata repository for dynamic column management.
        /// This table tracks all custom engineering parameters added to the partitioned main tables.
        /// </summary>
        public static async Task<bool> TableAsync_Columns(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return false;
            }

            // 'unique_id' is the actual physical column name in the PostgreSQL data table.
            // Using a composite UNIQUE constraint ensures that each physical column is documented only once per table.
            const string commandText = $@"
                CREATE TABLE IF NOT EXISTS {Constants.TableName.Columns} (
                    id          integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    table_name  text NOT NULL,
                    unique_id   text NOT NULL, -- Corresponds to the physical column name
                    name        text,          -- Friendly name (e.g. Revit Parameter Name)
                    description text,          -- Contextual info for LLM reasoning
                    category    text,          -- Grouping (e.g. Structural, Thermal, Identity)
                    unit        text,          -- Unit of measurement (e.g. meters, kilograms)
                    data        jsonb NOT NULL, -- Technical metadata (StorageType, UnitType, GUID)
                    created_at  timestamptz DEFAULT now(),

                    CONSTRAINT uq_table_column_identity UNIQUE(table_name, unique_id)
                );

                -- Index for fast lookup when checking if a column already exists before ALTER TABLE
                CREATE INDEX IF NOT EXISTS idx_columns_lookup_composite
                    ON {Constants.TableName.Columns} (table_name, unique_id);

                -- GIN index for metadata queries (internal technical filtering)
                CREATE INDEX IF NOT EXISTS idx_columns_data_jsonb
                    ON {Constants.TableName.Columns} USING GIN (data);";

            try
            {
                // Explicitly defining the command to maintain full control over the execution context
                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                await npgsqlCommand.ExecuteNonQueryAsync();

                return true;
            }
            catch (NpgsqlException npgsqlEx)
            {
                // Detailed error reporting essential for debugging long-running BIM export processes
                Console.WriteLine($"[Postgres Error] Code: {npgsqlEx.SqlState}, Message: {npgsqlEx.Message}");
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[General Error] Critical failure in SchemaManager: {ex.Message}");
                return false;
            }
        }
    }
}