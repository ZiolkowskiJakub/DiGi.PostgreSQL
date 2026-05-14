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
        protected abstract TableConversionOptions<UColumn>? tableConversionOptions { get; }

        public abstract string TableName { get; }

        public TablePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
 
        }

        public async Task<bool> UpdateAsync<TColumn, TRow>(Table<TColumn, TRow>? table, int batchSize = 1000) where TColumn : UColumn where TRow : IRow<TRow>
        {
            if (table is null || table.RowCount == 0)
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

            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            await CreateTableAsync(npgsqlConnection, dictionary.Values);

            StringBuilder stringBuilder = new();

            IEnumerable<string> columnNames = dictionary.Keys.Select(x => $"\"{x}\"");
            IEnumerable<string> parametersNames = dictionary.Keys.Select(x => $"@{x}");

            stringBuilder.AppendLine($"INSERT INTO \"{TableName}\" ({string.Join(", ", columnNames)})");
            stringBuilder.AppendLine($"VALUES ({string.Join(", ", parametersNames)})");

            if (tableConversionOptions?.PrimaryKeyColumns is List<UColumn> columns_PrimaryKey && columns_PrimaryKey.Count != 0)
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

                foreach (TRow row in table)
                {
                    NpgsqlBatchCommand npgsqlBatchCommand = new(commandText);

                    foreach (KeyValuePair<string, UColumn> keyValuePair in dictionary)
                    {
                        if (keyValuePair.Value?.NpgsqlDbType() is not NpgsqlDbType npgsqlDbType)
                        {
                            continue;
                        }

                        object parameterValue = row[keyValuePair.Value.Index] ?? DBNull.Value;
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
            catch (NpgsqlException npgsqlException)
            {
                await npgsqlTransaction.RollbackAsync();
                return false;
            }
        }

        private async Task<bool> CreateTableAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<UColumn> columns)
        {
            return await Create.TableAsync(npgsqlConnection, TableName, tableConversionOptions, columns);
        }
    }
}