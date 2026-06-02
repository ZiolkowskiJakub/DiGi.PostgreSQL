using DiGi.Core.IO.Table.Classes;
using DiGi.Core.IO.Table.Interfaces;
using DiGi.PostgreSQL.Classes;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.Table.Classes
{
    public abstract class TablePostgreSQLConverter<UColumn> : PostgreSQLConverter<Table<UColumn>> where UColumn : IColumn
    {
        public TablePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }

        public abstract string TableName { get; }

        protected abstract TableConversionOptions<UColumn>? TableConversionOptions { get; }

        public async Task<HashSet<string>?> GetCategories()
        {
            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            await npgsqlConnection.OpenAsync();

            return await GetCategories(npgsqlConnection);
        }

        public async Task<HashSet<string>?> GetCategories(NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            HashSet<string> categories = [];

            string query = $"SELECT category FROM \"{Constants.TableName.Columns}\" WHERE table_name = @tableName";

            await using NpgsqlCommand npgsqlCommand = new(query, npgsqlConnection);

            npgsqlCommand.Parameters.Add(new NpgsqlParameter("tableName", NpgsqlDbType.Text) { Value = TableName });

            await using NpgsqlDataReader reader = await npgsqlCommand.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                object? value = reader["category"];

                if (value == null || value == DBNull.Value)
                {
                    value = string.Empty;
                }

                categories.Add(value?.ToString() ?? string.Empty);
            }

            return categories;
        }

        public async Task<List<ColumnReference>?> GetColumnReferencesByCategories(IEnumerable<string>? categories = null)
        {
            return await GetColumnReferences("category", categories);
        }

        public async Task<List<ColumnReference>?> GetColumnReferencesByCategories(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? categories = null)
        {
            return await GetColumnReferences(npgsqlConnection, "category", categories);
        }

        public async Task<List<ColumnReference>?> GetColumnReferencesByNames(IEnumerable<string>? names = null)
        {
            return await GetColumnReferences("name", names);
        }

        public async Task<List<ColumnReference>?> GetColumnReferencesByNames(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? names = null)
        {
            return await GetColumnReferences(npgsqlConnection, "name", names);
        }

        public async Task<List<ColumnReference>?> GetColumnReferencesByUniqueIds(IEnumerable<string>? uniqueIds = null)
        {
            return await GetColumnReferences("unique_id", uniqueIds);
        }

        public async Task<List<ColumnReference>?> GetColumnReferencesByUniqueIds(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? uniqueIds = null)
        {
            return await GetColumnReferences(npgsqlConnection, "unique_id", uniqueIds);
        }

        public async Task<List<UColumn>?> GetColumns()
        {
            return await GetColumnsByUniqueIds();
        }

        public async Task<List<UColumn>?> GetColumnsByCategories(IEnumerable<string>? categories = null)
        {
            return await GetColumns("category", categories);
        }

        public async Task<List<UColumn>?> GetColumnsByCategories(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? categories = null)
        {
            return await GetColumns(npgsqlConnection, "category", categories);
        }

        public async Task<List<UColumn>?> GetColumnsByNames(IEnumerable<string>? names = null)
        {
            return await GetColumns("name", names);
        }

        public async Task<List<UColumn>?> GetColumnsByNames(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? names = null)
        {
            return await GetColumns(npgsqlConnection, "name", names);
        }

        public async Task<List<UColumn>?> GetColumnsByUniqueIds(IEnumerable<string>? uniqueIds = null)
        {
            return await GetColumns("unique_id", uniqueIds);
        }

        public async Task<List<UColumn>?> GetColumnsByUniqueIds(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? uniqueIds = null)
        {
            return await GetColumns(npgsqlConnection, "unique_id", uniqueIds);
        }

        public async Task<bool> PullAsync<TColumn, TRow>(NpgsqlConnection? npgsqlConnection, Table<TColumn, TRow>? table, int batchSize = 1000) where TColumn : UColumn where TRow : IRow<TRow>
        {
            if (table is null || npgsqlConnection is null)
            {
                return false;
            }

            if (table.Columns is not IEnumerable<TColumn> columns || !columns.Any())
            {
                return false;
            }

            Dictionary<string, TColumn> dictionary = [];
            foreach (TColumn column in columns)
            {
                if (column.UniqueId() is not string uniqueId || string.IsNullOrWhiteSpace(uniqueId))
                {
                    continue;
                }

                dictionary[uniqueId] = column;
            }

            IEnumerable<string> quotedColumns = dictionary.Keys.Select(x => $"\"{x}\"");
            string baseQuery = $"SELECT {string.Join(", ", quotedColumns)} FROM \"{TableName}\"";

            Dictionary<string, TColumn> dictionary_PrimaryKey = [];
            if (TableConversionOptions?.PrimaryKeyColumns is List<UColumn> columns_PrimaryKey && columns_PrimaryKey.Count != 0)
            {
                foreach (UColumn column_PrimaryKey in columns_PrimaryKey)
                {
                    if (column_PrimaryKey.UniqueId() is not string uniqueId || string.IsNullOrWhiteSpace(uniqueId))
                    {
                        continue;
                    }

                    if (!dictionary.TryGetValue(uniqueId, out TColumn? column))
                    {
                        continue;
                    }

                    dictionary_PrimaryKey[uniqueId] = column;
                }
            }

            // Map existing rows for quick lookup during merge
            Dictionary<string, TRow> existingRowsMap = [];
            if (dictionary_PrimaryKey.Count > 0 && table.Rows is IEnumerable<TRow> currentRows)
            {
                foreach (TRow row in currentRows)
                {
                    StringBuilder pkKeyBuilder = new();
                    foreach (TColumn pkCol in dictionary_PrimaryKey.Values)
                    {
                        pkKeyBuilder.Append(row[pkCol.Index]).Append('|');
                    }
                    existingRowsMap[pkKeyBuilder.ToString()] = row;
                }
            }

            // Case 1: Empty table or no PKs defined -> Pull all data
            if (table.RowCount == 0 || dictionary_PrimaryKey.Count == 0)
            {
                await using NpgsqlCommand npgsqlCommand = new(baseQuery, npgsqlConnection);
                await using NpgsqlDataReader reader = await npgsqlCommand.ExecuteReaderAsync();
                return await ReadAsync(reader, table, dictionary, dictionary_PrimaryKey, existingRowsMap);
            }

            // Case 2: Non-empty table with PKs -> Pull only matching data in batches
            List<TRow> rowsList = [.. table.Rows];
            for (int i = 0; i < rowsList.Count; i += batchSize)
            {
                List<TRow> batch = [.. rowsList.Skip(i).Take(batchSize)];
                StringBuilder whereClause = new();
                whereClause.Append(" WHERE ");

                List<NpgsqlParameter> parameters = [];
                for (int j = 0; j < batch.Count; j++)
                {
                    if (j > 0)
                    {
                        whereClause.Append(" OR ");
                    }
                    whereClause.Append('(');

                    TRow row = batch[j];
                    int paramIdx = 0;
                    foreach (TColumn pkCol in dictionary_PrimaryKey.Values)
                    {
                        string paramName = $"@p{i}_{j}_{paramIdx}";
                        if (paramIdx > 0)
                        {
                            whereClause.Append(" AND ");
                        }
                        whereClause.Append($"\"{pkCol.UniqueId()}\" = {paramName}");
                        parameters.Add(new NpgsqlParameter(paramName, row[pkCol.Index] ?? DBNull.Value));
                        paramIdx++;
                    }
                    whereClause.Append(')');
                }

                await using NpgsqlCommand npgsqlCommand = new(baseQuery + whereClause.ToString(), npgsqlConnection);
                npgsqlCommand.Parameters.AddRange(parameters.ToArray());
                await using NpgsqlDataReader reader = await npgsqlCommand.ExecuteReaderAsync();
                if (!await ReadAsync(reader, table, dictionary, dictionary_PrimaryKey, existingRowsMap))
                {
                    return false;
                }
            }

            return true;
        }

        public async Task<bool> PullAsync<TColumn, TRow>(Table<TColumn, TRow>? table, int batchSize = 1000) where TColumn : UColumn where TRow : IRow<TRow>
        {
            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            return await PullAsync(npgsqlConnection, table, batchSize);
        }

        public async Task<bool> PushAsync<TColumn, TRow>(Table<TColumn, TRow>? table, int batchSize = 1000) where TColumn : UColumn where TRow : IRow<TRow>
        {
            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            return await PushAsync(npgsqlConnection, table, batchSize);
        }

        public async Task<bool> PushAsync<TColumn, TRow>(NpgsqlConnection? npgsqlConnection, Table<TColumn, TRow>? table, int batchSize = 1000) where TColumn : UColumn where TRow : IRow<TRow>
        {
            if (table is null || table.RowCount == 0 || npgsqlConnection is null)
            {
                return false;
            }

            if (table.Columns is not IEnumerable<TColumn> columns || !columns.Any())
            {
                return false;
            }

            Dictionary<string, UColumn> dictionary = [];
            foreach (TColumn column in columns)
            {
                if (column?.UniqueId() is not string uniqueId)
                {
                    continue;
                }

                dictionary[uniqueId] = column;
            }

            if (dictionary.Count == 0)
            {
                return false;
            }

            TColumn? partitionColumn = default;

            if (TableConversionOptions is not null)
            {
                if (TableConversionOptions.IdentityColumn is UColumn identityColumn)
                {
                    if (identityColumn?.UniqueId() is string uniqueId && !string.IsNullOrWhiteSpace(uniqueId))
                    {
                        if (dictionary.TryGetValue(uniqueId, out UColumn? column) && column is not null)
                        {
                            identityColumn.Index = column.Index;
                            dictionary[uniqueId] = identityColumn;
                        }
                    }
                }

                if (TableConversionOptions.UniqueColumns is List<UColumn> uniqueColumns)
                {
                    foreach (UColumn uniqueColumn in uniqueColumns)
                    {
                        if (uniqueColumn?.UniqueId() is string uniqueId && !string.IsNullOrWhiteSpace(uniqueId))
                        {
                            if (dictionary.TryGetValue(uniqueId, out UColumn? column) && column is not null)
                            {
                                uniqueColumn.Index = column.Index;
                                dictionary[uniqueId] = uniqueColumn;
                            }
                        }
                    }
                }

                if (TableConversionOptions.PartitioningOptions is PartitioningOptions<UColumn> partitioningOptions)
                {
                    if (partitioningOptions.Column?.UniqueId() is string uniqueId && !string.IsNullOrWhiteSpace(uniqueId))
                    {
                        if (dictionary.TryGetValue(uniqueId, out UColumn? column) && column is not null && partitioningOptions.Column is TColumn partitionColumn_Temp)
                        {
                            partitionColumn = partitionColumn_Temp;
                            partitionColumn_Temp.Index = column.Index;
                            dictionary[uniqueId] = partitionColumn_Temp;
                        }
                    }
                }

                if (TableConversionOptions.PrimaryKeyColumns is List<UColumn> primaryKeyColumns)
                {
                    foreach (UColumn primaryKeyColumn in primaryKeyColumns)
                    {
                        if (primaryKeyColumn?.UniqueId() is string uniqueId && !string.IsNullOrWhiteSpace(uniqueId))
                        {
                            if (dictionary.TryGetValue(uniqueId, out UColumn? column) && column is not null)
                            {
                                primaryKeyColumn.Index = column.Index;
                                dictionary[uniqueId] = primaryKeyColumn;
                            }
                        }
                    }
                }
            }

            await CreateTableAsync(npgsqlConnection, dictionary.Values);

            StringBuilder stringBuilder = new();

            IEnumerable<string> columnNames = dictionary.Keys.Select(x => $"\"{x}\"");
            IEnumerable<string> parametersNames = dictionary.Keys.Select(x => $"@{x}");

            stringBuilder.AppendLine($"INSERT INTO \"{TableName}\" ({string.Join(", ", columnNames)})");
            stringBuilder.AppendLine($"VALUES ({string.Join(", ", parametersNames)})");

            if (TableConversionOptions?.PrimaryKeyColumns is List<UColumn> columns_PrimaryKey && columns_PrimaryKey.Count != 0)
            {
                columns_PrimaryKey.RemoveAll(x => x?.UniqueId() is not string uniqueId || !dictionary.ContainsKey(uniqueId));

                if (columns_PrimaryKey.Count > 0)
                {
                    Dictionary<string, UColumn> dictionary_PrimaryKey = [];
                    Dictionary<string, UColumn> dictionary_Other = [];

                    foreach (KeyValuePair<string, UColumn> keyValuePair in dictionary)
                    {
                        if (columns_PrimaryKey.FindIndex(x => x.UniqueId() == keyValuePair.Key) != -1)
                        {
                            dictionary_PrimaryKey[keyValuePair.Key] = keyValuePair.Value;
                        }
                        else
                        {
                            dictionary_Other[keyValuePair.Key] = keyValuePair.Value;
                        }
                    }

                    if (dictionary_PrimaryKey.Count > 0 && dictionary_Other.Count > 0)
                    {
                        IEnumerable<string> primaryKeysQuoted = dictionary_PrimaryKey.Keys.Select(x => $"\"{x}\"");
                        List<string> lines = [];

                        foreach (KeyValuePair<string, UColumn> keyValuePair in dictionary_Other)
                        {
                            lines.Add($"\"{keyValuePair.Key}\" = EXCLUDED.\"{keyValuePair.Key}\"");
                        }

                        stringBuilder.AppendLine($"ON CONFLICT ({string.Join(", ", primaryKeysQuoted)})");
                        stringBuilder.AppendLine($"DO UPDATE SET {string.Join(", ", lines)}");
                    }
                }
            }

            stringBuilder.Append(';');
            string commandText = stringBuilder.ToString();

            int rowCounter = 0;

            await using NpgsqlTransaction npgsqlTransaction = await npgsqlConnection.BeginTransactionAsync();

            try
            {
                NpgsqlBatch npgsqlBatch = new(npgsqlConnection, npgsqlTransaction);

                if (partitionColumn is not null)
                {
                    object?[]? values = table.GetColumnValues(partitionColumn);
                    if (values is not null)
                    {
                        HashSet<object?> values_Temp = [.. values];

                        foreach (object? value in values_Temp)
                        {
                            string? partitionSufix = Query.PartitionNameSuffix(value);
                            if (string.IsNullOrWhiteSpace(partitionSufix))
                            {
                                await PostgreSQL.Create.TableAsync_Partition_Default(npgsqlConnection, TableName);
                            }
                            else
                            {
                                await PostgreSQL.Create.TableAsync_Partition(npgsqlConnection, TableName, partitionSufix, [value]);
                            }
                        }
                    }
                }

                foreach (TRow row in table)
                {
                    NpgsqlBatchCommand npgsqlBatchCommand = new(commandText);

                    foreach (KeyValuePair<string, UColumn> keyValuePair in dictionary)
                    {
                        if (keyValuePair.Value?.NpgsqlDbType() is not NpgsqlDbType npgsqlDbType)
                        {
                            continue;
                        }

                        if (!Query.TryConvert(row[keyValuePair.Value.Index], out object? parameterValue, npgsqlDbType) || parameterValue is null)
                        {
                            parameterValue = DBNull.Value;
                        }

                        npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter(keyValuePair.Key, npgsqlDbType) { Value = parameterValue });
                    }

                    npgsqlBatch.BatchCommands.Add(npgsqlBatchCommand);
                    rowCounter++;

                    if (rowCounter % batchSize == 0)
                    {
                        await npgsqlBatch.ExecuteNonQueryAsync();
                        await npgsqlBatch.DisposeAsync();

                        npgsqlBatch = new NpgsqlBatch(npgsqlConnection, npgsqlTransaction);
                    }
                }

                if (npgsqlBatch.BatchCommands.Count > 0)
                {
                    await npgsqlBatch.ExecuteNonQueryAsync();
                    await npgsqlBatch.DisposeAsync();
                }

                await npgsqlTransaction.CommitAsync();

                await Modify.UpdateAsync(npgsqlConnection, TableName, dictionary.Values);

                return true;
            }
            catch (NpgsqlException)
            {
                await npgsqlTransaction.RollbackAsync();
                return false;
            }
        }

        private static async Task<bool> ReadAsync<TColumn, TRow>(NpgsqlDataReader npgsqlDataReader, Table<TColumn, TRow> table, Dictionary<string, TColumn> dictionary, Dictionary<string, TColumn> dictionary_PrimaryKey, Dictionary<string, TRow> existingRowsMap) where TColumn : IColumn where TRow : IRow<TRow>
        {
            while (await npgsqlDataReader.ReadAsync())
            {
                Dictionary<string, object?> values = [];
                foreach (KeyValuePair<string, TColumn> keyValuePair in dictionary)
                {
                    values[keyValuePair.Value.UniqueId()!] = npgsqlDataReader[keyValuePair.Key];
                }

                if (dictionary_PrimaryKey.Count > 0)
                {
                    StringBuilder stringBuilder = new();
                    foreach (TColumn column in dictionary_PrimaryKey.Values)
                    {
                        stringBuilder.Append(npgsqlDataReader[column.UniqueId()!]).Append('|');
                    }
                    string uniqueValue = stringBuilder.ToString();

                    if (existingRowsMap.TryGetValue(uniqueValue, out TRow? row_Existing))
                    {
                        foreach (KeyValuePair<string, TColumn> keyValuePair in dictionary)
                        {
                            object? value_Existing = row_Existing[keyValuePair.Value.Index];
                            object? value_New = values[keyValuePair.Key];
                            if ((value_Existing == null || value_Existing.Equals(Core.Query.Default(keyValuePair.Value.Type))) && value_New != null)
                            {
                                row_Existing[keyValuePair.Value.Index] = value_New;
                            }
                        }

                        table.AddRow(row_Existing);
                    }
                    else
                    {
                        table.AddRow(values);
                    }
                }
                else
                {
                    table.AddRow(values);
                }
            }
            return true;
        }

        private async Task<bool> CreateTableAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<UColumn> columns)
        {
            return await Create.TableAsync(npgsqlConnection, TableName, TableConversionOptions, columns);
        }

        private async Task<List<ColumnReference>?> GetColumnReferences(NpgsqlConnection? npgsqlConnection, string columnName, IEnumerable<string>? values = null)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            List<ColumnReference> result = [];

            // Build query based on whether values are provided
            string query = $"SELECT id, table_name, unique_id, name, description, category FROM \"{Constants.TableName.Columns}\" WHERE table_name = @tableName";

            bool hasFilter = values != null && values.Any();
            if (hasFilter)
            {
                query += $" AND {columnName} = ANY(@{columnName})";
            }

            await using NpgsqlCommand npgsqlCommand = new(query, npgsqlConnection);
            npgsqlCommand.Parameters.Add(new NpgsqlParameter("tableName", NpgsqlDbType.Text) { Value = TableName });

            if (hasFilter)
            {
                // Pass values as a PostgreSQL array for the ANY operator
                npgsqlCommand.Parameters.Add(new NpgsqlParameter($"{columnName}", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = values!.ToArray() });
            }

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();
            while (await npgsqlDataReader.ReadAsync())
            {
                ColumnReference columnReference = new()
                {
                    Id = npgsqlDataReader["id"] is int id ? id : -1,
                    TableName = npgsqlDataReader["table_name"] as string,
                    UniqueId = npgsqlDataReader["unique_id"] as string,
                    Name = npgsqlDataReader["name"] as string,
                    Description = npgsqlDataReader["description"] as string,
                    Category = npgsqlDataReader["category"] as string
                };

                result.Add(columnReference);
            }

            return result;
        }

        private async Task<List<ColumnReference>?> GetColumnReferences(string columnName, IEnumerable<string>? values = null)
        {
            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);

            if (npgsqlConnection is null)
            {
                return null;
            }

            await npgsqlConnection.OpenAsync();

            return await GetColumnReferences(columnName, values);
        }

        private async Task<List<UColumn>?> GetColumns(string columnName, IEnumerable<string>? values = null)
        {
            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);

            if (npgsqlConnection is null)
            {
                return null;
            }

            await npgsqlConnection.OpenAsync();

            return await GetColumns(npgsqlConnection, columnName, values);
        }

        private async Task<List<UColumn>?> GetColumns(NpgsqlConnection? npgsqlConnection, string columnName, IEnumerable<string>? values = null)
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            List<UColumn> columns = [];

            // Build query based on whether values are provided
            string query = $"SELECT data FROM \"{Constants.TableName.Columns}\" WHERE table_name = @tableName";

            bool hasFilter = values != null && values.Any();
            if (hasFilter)
            {
                query += $" AND {columnName} = ANY(@{columnName})";
            }

            await using NpgsqlCommand npgsqlCommand = new(query, npgsqlConnection);
            npgsqlCommand.Parameters.Add(new NpgsqlParameter("tableName", NpgsqlDbType.Text) { Value = TableName });

            if (hasFilter)
            {
                // Pass values as a PostgreSQL array for the ANY operator
                npgsqlCommand.Parameters.Add(new NpgsqlParameter($"{columnName}", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = values!.ToArray() });
            }

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();
            while (await npgsqlDataReader.ReadAsync())
            {
                object? @object = npgsqlDataReader["data"];
                if (@object != null && @object != DBNull.Value)
                {
                    string json = @object.ToString() ?? string.Empty;

                    // Convert the JSON metadata back to a UColumn object using Core utility
                    List<UColumn>? columns_Temp = Core.Convert.ToDiGi<UColumn>(json);
                    if (columns_Temp != null && columns_Temp.Count != 0)
                    {
                        columns.Add(columns_Temp[0]);
                    }
                }
            }

            return columns;
        }
    }
}