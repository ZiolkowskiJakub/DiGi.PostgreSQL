using DiGi.Core.IO.Table.Interfaces;
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
        /// <summary>
        /// Asynchronously creates a table or adds missing columns to an existing table in the PostgreSQL database based on the provided column definitions and options.
        /// </summary>
        /// <typeparam name="UColumn">The type of column implementation, which must implement <see cref="IColumn"/>.</typeparam>
        /// <param name="npgsqlConnection">The Npgsql connection instance used to execute the database commands.</param>
        /// <param name="tableName">The name of the table to be created or modified.</param>
        /// <param name="tableConversionOptions">Optional configuration settings for table conversion, such as primary keys and partitioning rules.</param>
        /// <param name="columns">A collection of column definitions to be applied to the table.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the table was successfully created or updated; otherwise, false.</returns>
        public static async Task<bool> TableAsync<UColumn>(this NpgsqlConnection? npgsqlConnection, string tableName, TableConversionOptions<UColumn>? tableConversionOptions, IEnumerable<UColumn> columns) where UColumn : IColumn
        {
            if (string.IsNullOrWhiteSpace(tableName) || npgsqlConnection is null)
            {
                return false;
            }

            StringBuilder stringBuilder = new();
            List<UColumn>? columns_New = null;

            List<string>? uniqueIds = await PostgreSQL.Query.ColumnNamesAsync(npgsqlConnection, tableName);
            if (uniqueIds is null || uniqueIds.Count == 0)
            {
                // Table does not exist - Create new table structure
                await TableAsync_Columns(npgsqlConnection);

                Dictionary<string, UColumn> dictionary_All = [];
                HashSet<string> uniqueIds_PrimaryKey = [];
                HashSet<string> uniqueIds_Unique = [];

                if (tableConversionOptions is not null)
                {
                    if (tableConversionOptions.IdentityColumn is UColumn column_Identity && column_Identity.UniqueId() is string uniqueId_Identity && !string.IsNullOrWhiteSpace(uniqueId_Identity))
                    {
                        dictionary_All[uniqueId_Identity] = column_Identity;
                    }

                    if (tableConversionOptions.PrimaryKeyColumns is List<UColumn> columns_TableConversionOptions_PrimaryKey)
                    {
                        foreach (UColumn column in columns_TableConversionOptions_PrimaryKey)
                        {
                            if (column?.UniqueId() is string uniqueId && !string.IsNullOrWhiteSpace(uniqueId))
                            {
                                dictionary_All[uniqueId] = column;
                                uniqueIds_PrimaryKey.Add(uniqueId);
                            }
                        }
                    }

                    if (tableConversionOptions.PartitioningOptions is PartitioningOptions<UColumn> partitioningOptions)
                    {
                        if (partitioningOptions.Column?.UniqueId() is string uniqueId && !string.IsNullOrWhiteSpace(uniqueId))
                        {
                            dictionary_All[uniqueId] = partitioningOptions.Column;
                            // Enforce that partitioning column is part of the primary key in PostgreSQL
                            uniqueIds_PrimaryKey.Add(uniqueId);
                        }
                    }

                    if (tableConversionOptions.UniqueColumns is List<UColumn> columns_TableConversionOptions_Unique)
                    {
                        foreach (UColumn column in columns_TableConversionOptions_Unique)
                        {
                            if (column?.UniqueId() is string uniqueId && !string.IsNullOrWhiteSpace(uniqueId))
                            {
                                dictionary_All[uniqueId] = column;
                                uniqueIds_Unique.Add(uniqueId);
                            }
                        }
                    }
                }

                List<UColumn> columns_PrimaryKey = [];
                List<UColumn> columns_Unique = [];
                List<UColumn> columns_Other = [];

                foreach (KeyValuePair<string, UColumn> keyValuePair in dictionary_All)
                {
                    if (uniqueIds_PrimaryKey.Contains(keyValuePair.Key))
                    {
                        columns_PrimaryKey.Add(keyValuePair.Value);
                    }
                    else if (uniqueIds_Unique.Contains(keyValuePair.Key))
                    {
                        columns_Unique.Add(keyValuePair.Value);
                    }
                    else
                    {
                        columns_Other.Add(keyValuePair.Value);
                    }
                }

                if (columns is not null)
                {
                    foreach (UColumn column in columns)
                    {
                        if (column?.UniqueId() is not string uniqueId || string.IsNullOrWhiteSpace(uniqueId))
                        {
                            continue;
                        }

                        if (!dictionary_All.ContainsKey(uniqueId))
                        {
                            dictionary_All[uniqueId] = column;
                            columns_Other.Add(column);
                        }
                    }
                }

                List<UColumn> columns_All = [.. dictionary_All.Values];
                columns_All.Sort((x, y) => x.Index.CompareTo(y.Index));

                List<string> lines = [];
                foreach (UColumn column in columns_All)
                {
                    if (column?.UniqueId() is not string uniqueId)
                    {
                        continue;
                    }

                    if (column.DataTypeName() is not string dataTypeName || string.IsNullOrWhiteSpace(dataTypeName))
                    {
                        continue;
                    }

                    // Escaping column identifiers to prevent injection and avoid PostgreSQL case-sensitivity mismatch
                    string line = $"\"{uniqueId}\" {dataTypeName}";

                    if (columns_PrimaryKey is not null && columns_PrimaryKey.Find(x => x.UniqueId() == uniqueId) is not null)
                    {
                        line += " NOT NULL";
                    }

                    lines.Add(line);
                }

                stringBuilder.Append($"CREATE TABLE \"{tableName}\" (");
                stringBuilder.Append(string.Join(", ", lines));

                if (columns_PrimaryKey is not null && columns_PrimaryKey.Count != 0)
                {
                    columns_PrimaryKey.Sort((x, y) => x.Index.CompareTo(y.Index));

                    lines.Clear(); // Reuse list to save allocations
                    foreach (UColumn column in columns_PrimaryKey)
                    {
                        if (column?.UniqueId() is not string uniqueId)
                        {
                            continue;
                        }

                        lines.Add($"\"{uniqueId}\"");
                    }

                    if (lines.Count > 0)
                    {
                        stringBuilder.Append($", PRIMARY KEY ({string.Join(", ", lines)})");
                    }
                }

                if (columns_Unique is not null && columns_Unique.Count != 0)
                {
                    columns_Unique.Sort((x, y) => x.Index.CompareTo(y.Index));

                    lines.Clear(); // Reuse list to save allocations
                    foreach (UColumn column in columns_Unique)
                    {
                        if (column?.UniqueId() is not string uniqueId)
                        {
                            continue;
                        }

                        lines.Add($"\"{uniqueId}\"");
                    }

                    if (lines.Count > 0)
                    {
                        stringBuilder.Append($", UNIQUE ({string.Join(", ", lines)})");
                    }
                }

                stringBuilder.Append(')');

                if (tableConversionOptions is not null)
                {
                    if (tableConversionOptions.PartitioningOptions is PartitioningOptions<UColumn> partitioningOptions && partitioningOptions.PartitioningRule is PartitioningRule partitioningRule)
                    {
                        if (partitioningOptions.Column?.UniqueId() is string uniqueId && !string.IsNullOrWhiteSpace(uniqueId))
                        {
                            // Fixed: Cleaned expression structure for PARTITION BY statement to prevent syntax errors
                            if (partitioningRule is ValuePartitioningRule)
                            {
                                stringBuilder.Append($" PARTITION BY LIST (\"{uniqueId}\")");
                            }
                            else if (partitioningRule is RangePartitioningRule)
                            {
                                stringBuilder.Append($" PARTITION BY RANGE (\"{uniqueId}\")");
                            }
                        }
                    }
                }

                stringBuilder.Append(';');
            }
            else
            {
                // Table exists - Add missing columns only
                Dictionary<string, UColumn> dictionary = [];
                foreach (UColumn column in columns)
                {
                    if (column?.UniqueId() is not string uniqueId)
                    {
                        continue;
                    }

                    if (uniqueIds.Contains(uniqueId))
                    {
                        continue;
                    }

                    dictionary[uniqueId] = column;
                }

                columns_New = [.. dictionary.Values];
                columns_New.Sort((x, y) => x.Index.CompareTo(y.Index));

                await Modify.UpdateAsync(npgsqlConnection, tableName, columns_New);

                List<string> definitions = [];
                foreach (UColumn column_New in columns_New)
                {
                    if (column_New?.UniqueId() is not string uniqueId)
                    {
                        continue;
                    }

                    if (column_New.DataTypeName() is not string dataTypeName || string.IsNullOrWhiteSpace(dataTypeName))
                    {
                        continue;
                    }

                    // Fixed: Added escaped identifiers for explicit safety
                    definitions.Add($"ADD COLUMN IF NOT EXISTS \"{uniqueId}\" {dataTypeName}");
                }

                if (definitions.Count > 0)
                {
                    stringBuilder.Append($"ALTER TABLE \"{tableName}\" ");
                    stringBuilder.Append(string.Join(", ", definitions));
                    stringBuilder.Append(';');
                }
            }

            string commandText = stringBuilder.ToString();

            if (string.IsNullOrWhiteSpace(commandText))
            {
                return false;
            }

            await using NpgsqlTransaction transaction = await npgsqlConnection.BeginTransactionAsync();
            try
            {
                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection, transaction);
                await npgsqlCommand.ExecuteNonQueryAsync();

                await transaction.CommitAsync();
                return true;
            }
            catch (NpgsqlException npgsqlException)
            {
                await transaction.RollbackAsync();
                Console.WriteLine($"{nameof(TableAsync)} failed: {npgsqlException.Message} (State: {npgsqlException.SqlState})");
                return false;
            }
        }

        /// <summary>
        /// Initializes the metadata repository for dynamic column management.
        /// This table tracks all custom engineering parameters added to the partitioned main tables.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection instance used to create the columns metadata table.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the repository was successfully initialized; otherwise, false.</returns>
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