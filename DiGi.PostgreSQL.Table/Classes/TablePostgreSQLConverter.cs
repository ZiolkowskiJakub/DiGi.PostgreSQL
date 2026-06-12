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
    /// <summary>
    /// Provides an abstract base class for converters that handle the translation between a <see cref="Table{T}"/>
    /// and its PostgreSQL representation.
    /// </summary>
    /// <typeparam name="UColumn">The type of columns contained within the table, which must implement the <see cref="IColumn"/> interface.</typeparam>
    public abstract class TablePostgreSQLConverter<UColumn> : PostgreSQLConverter<Table<UColumn>> where UColumn : IColumn
    {
        /// <summary>
        /// Initializes a new instance of the TablePostgreSQLConverter class using the specified connection data.
        /// </summary>
        /// <param name="connectionData">The connection configuration details used to connect to the PostgreSQL database. This value can be null.</param>
        public TablePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }

        /// <summary>
        /// Gets the name of the database table associated with this entity.
        /// </summary>
        /// <returns>The string representing the name of the database table.</returns>
        public abstract string TableName { get; }

        /// <summary>
        /// Gets the options used to configure the table conversion process.
        /// </summary>
        /// <returns>The configuration options for the table conversion, or <c>null</c> if no specific options are provided.</returns>
        protected abstract TableConversionOptions<UColumn>? TableConversionOptions { get; }

        /// <summary>
        /// Samples partition data to dynamically detect the most common separator (comma, semicolon, or pipe).
        /// <para>Resolves partitioning settings dynamically from <see cref="TableConversionOptions"/>.</para>
        /// </summary>
        /// <param name="npgsqlConnection">The active database connection instance.</param>
        /// <param name="columnUniqueId">The unique identifier of the column to sample.</param>
        /// <param name="partitionValue">The partition key value; ignored if partitioning is disabled.</param>
        /// <returns>A task representing the async operation, returning the detected separator character string (e.g. ",", ";", or "|").</returns>
        public async Task<string> DetectSeparatorAsync(NpgsqlConnection npgsqlConnection, string columnUniqueId, object? partitionValue = null)
        {
            string? partitionColumnUniqueId = TableConversionOptions?.PartitioningOptions?.Column?.UniqueId();
            bool hasPartition = !string.IsNullOrEmpty(partitionColumnUniqueId) && partitionValue != null;

            string commandText = $@"
                SELECT
                    coalesce(sum(length(""{columnUniqueId}"") - length(replace(""{columnUniqueId}"", ',', ''))), 0) as count_comma,
                    coalesce(sum(length(""{columnUniqueId}"") - length(replace(""{columnUniqueId}"", ';', ''))), 0) as count_semi,
                    coalesce(sum(length(""{columnUniqueId}"") - length(replace(""{columnUniqueId}"", '|', ''))), 0) as count_pipe
                FROM (
                    SELECT ""{columnUniqueId}""
                    FROM ""{TableName}""
                    {(hasPartition ? $"WHERE \"{partitionColumnUniqueId}\" = @partitionValue" : "WHERE 1=1")}
                      AND ""{columnUniqueId}"" IS NOT NULL
                    LIMIT 50
                ) s;";

            await using NpgsqlCommand npgsqlCommand_Detect = new NpgsqlCommand(commandText, npgsqlConnection);
            if (hasPartition)
            {
                npgsqlCommand_Detect.Parameters.AddWithValue("partitionValue", partitionValue!);
            }

            await using NpgsqlDataReader npgsqlDataReader_Detect = await npgsqlCommand_Detect.ExecuteReaderAsync();
            if (await npgsqlDataReader_Detect.ReadAsync())
            {
                long countComma = npgsqlDataReader_Detect.GetInt64(0);
                long countSemi = npgsqlDataReader_Detect.GetInt64(1);
                long countPipe = npgsqlDataReader_Detect.GetInt64(2);

                if (countSemi > countComma && countSemi > countPipe)
                {
                    return ";";
                }
                if (countPipe > countComma && countPipe > countSemi)
                {
                    return "|";
                }
            }
            return ","; // Default separator
        }

        /// <summary>
        /// Computes single-value aggregate statistics on a specific column in a partition.
        /// <para>Resolves partitioning settings dynamically from <see cref="TableConversionOptions"/>.</para>
        /// </summary>
        /// <typeparam name="TColumn">The column type implementation.</typeparam>
        /// <param name="npgsqlConnection">The active database connection instance.</param>
        /// <param name="columnUniqueId">The unique identifier of the column to aggregate.</param>
        /// <param name="singlevalueAggregateFunction">The single-value aggregation function to perform.</param>
        /// <param name="partitionValue">The partition key value; ignored if partitioning is disabled.</param>
        /// <returns>A task representing the async operation, returning the aggregation result as a <see cref="System.Text.Json.Nodes.JsonNode"/>.</returns>
        public async Task<System.Text.Json.Nodes.JsonNode?> GetAggregateSummaryAsync<TColumn>(NpgsqlConnection npgsqlConnection, string columnUniqueId, Enums.SinglevalueAggregateFunction singlevalueAggregateFunction, object? partitionValue = null)
            where TColumn : UColumn
        {
            // 1. Column Whitelist Validation to prevent SQL injection
            System.Collections.Generic.List<UColumn>? existingColumns = await GetColumnsByUniqueIdsAsync(npgsqlConnection, [columnUniqueId]);
            if (existingColumns is null || existingColumns.Count == 0)
            {
                return null;
            }

            // Resolve partitioning column from conversion options
            string? partitionColumnUniqueId = TableConversionOptions?.PartitioningOptions?.Column?.UniqueId();
            bool hasPartition = !string.IsNullOrEmpty(partitionColumnUniqueId) && partitionValue != null;

            string sqlFunc = singlevalueAggregateFunction switch
            {
                Enums.SinglevalueAggregateFunction.Avg => $"AVG(\"{columnUniqueId}\")",
                Enums.SinglevalueAggregateFunction.Sum => $"SUM(\"{columnUniqueId}\")",
                Enums.SinglevalueAggregateFunction.Min => $"MIN(\"{columnUniqueId}\")",
                Enums.SinglevalueAggregateFunction.Max => $"MAX(\"{columnUniqueId}\")",
                Enums.SinglevalueAggregateFunction.Count => "COUNT(*)",
                Enums.SinglevalueAggregateFunction.DistinctCount => $"COUNT(DISTINCT \"{columnUniqueId}\")",
                _ => throw new System.ComponentModel.InvalidEnumArgumentException()
            };

            string commandText = $@"
                SELECT {sqlFunc}
                FROM ""{TableName}""
                WHERE {(hasPartition ? $"\"{partitionColumnUniqueId}\" = @partitionValue" : "1=1")}";

            await using NpgsqlCommand npgsqlCommand_Aggregate = new NpgsqlCommand(commandText, npgsqlConnection);
            if (hasPartition)
            {
                npgsqlCommand_Aggregate.Parameters.AddWithValue("partitionValue", partitionValue!);
            }

            object? resultValue = await npgsqlCommand_Aggregate.ExecuteScalarAsync();
            return System.Text.Json.Nodes.JsonValue.Create(resultValue == System.DBNull.Value ? null : resultValue);
        }

        /// <summary>
        /// Computes multi-value aggregate statistics on a specific column in a partition.
        /// <para>Resolves partitioning settings dynamically from <see cref="TableConversionOptions"/>.</para>
        /// </summary>
        /// <typeparam name="TColumn">The column type implementation.</typeparam>
        /// <param name="npgsqlConnection">The active database connection instance.</param>
        /// <param name="columnUniqueId">The unique identifier of the column to aggregate.</param>
        /// <param name="multivalueAggregateFunction">The multi-value aggregation function to perform.</param>
        /// <param name="partitionValue">The partition key value; ignored if partitioning is disabled.</param>
        /// <param name="separator">The custom separator character; if null, it is dynamically detected.</param>
        /// <returns>A task representing the async operation, returning the aggregation result as a <see cref="System.Text.Json.Nodes.JsonNode"/>.</returns>
        public async Task<System.Text.Json.Nodes.JsonNode?> GetAggregateSummaryAsync<TColumn>(NpgsqlConnection npgsqlConnection, string columnUniqueId, Enums.MultivalueAggregateFunction multivalueAggregateFunction, object? partitionValue = null, string? separator = null)
            where TColumn : UColumn
        {
            // 1. Column Whitelist Validation to prevent SQL injection
            System.Collections.Generic.List<UColumn>? existingColumns = await GetColumnsByUniqueIdsAsync(npgsqlConnection, [columnUniqueId]);
            if (existingColumns is null || existingColumns.Count == 0)
            {
                return null;
            }

            // Resolve partitioning column from conversion options
            string? partitionColumnUniqueId = TableConversionOptions?.PartitioningOptions?.Column?.UniqueId();
            bool hasPartition = !string.IsNullOrEmpty(partitionColumnUniqueId) && partitionValue != null;

            string commandText;
            if (multivalueAggregateFunction == Enums.MultivalueAggregateFunction.SplitValueDistribution || multivalueAggregateFunction == Enums.MultivalueAggregateFunction.SplitDistinctCount)
            {
                if (multivalueAggregateFunction == Enums.MultivalueAggregateFunction.SplitValueDistribution)
                {
                    commandText = $@"
                        SELECT trim(both ' ' from unnested_item) as item, count(*) as count
                        FROM (
                            SELECT unnest(string_to_array(""{columnUniqueId}"", @separator)) as unnested_item
                            FROM ""{TableName}""
                            WHERE {(hasPartition ? $"\"{partitionColumnUniqueId}\" = @partitionValue" : "1=1")} AND ""{columnUniqueId}"" IS NOT NULL
                        ) subquery
                        GROUP BY item
                        ORDER BY count DESC;";
                }
                else
                {
                    commandText = $@"
                        SELECT count(DISTINCT trim(both ' ' from unnested_item))
                        FROM (
                            SELECT unnest(string_to_array(""{columnUniqueId}"", @separator)) as unnested_item
                            FROM ""{TableName}""
                            WHERE {(hasPartition ? $"\"{partitionColumnUniqueId}\" = @partitionValue" : "1=1")} AND ""{columnUniqueId}"" IS NOT NULL
                        ) subquery;";
                }
            }
            else
            {
                throw new System.ComponentModel.InvalidEnumArgumentException();
            }

            await using NpgsqlCommand npgsqlCommand_Aggregate = new NpgsqlCommand(commandText, npgsqlConnection);
            if (hasPartition)
            {
                npgsqlCommand_Aggregate.Parameters.AddWithValue("partitionValue", partitionValue!);
            }
            if (multivalueAggregateFunction == Enums.MultivalueAggregateFunction.SplitValueDistribution || multivalueAggregateFunction == Enums.MultivalueAggregateFunction.SplitDistinctCount)
            {
                string actualSeparator = separator ?? string.Empty;
                if (string.IsNullOrEmpty(actualSeparator))
                {
                    actualSeparator = await DetectSeparatorAsync(npgsqlConnection, columnUniqueId, partitionValue);
                }
                npgsqlCommand_Aggregate.Parameters.AddWithValue("separator", actualSeparator);
            }

            if (multivalueAggregateFunction == Enums.MultivalueAggregateFunction.SplitValueDistribution)
            {
                await using NpgsqlDataReader npgsqlDataReader_Distribution = await npgsqlCommand_Aggregate.ExecuteReaderAsync();
                System.Text.Json.Nodes.JsonArray jsonArray_Result = new System.Text.Json.Nodes.JsonArray();
                while (await npgsqlDataReader_Distribution.ReadAsync())
                {
                    System.Text.Json.Nodes.JsonObject jsonObject_Item = new System.Text.Json.Nodes.JsonObject();
                    jsonObject_Item["item"] = npgsqlDataReader_Distribution.GetString(0);
                    jsonObject_Item["count"] = npgsqlDataReader_Distribution.GetInt64(1);
                    jsonArray_Result.Add(jsonObject_Item);
                }
                return jsonArray_Result;
            }
            else
            {
                object? resultValue = await npgsqlCommand_Aggregate.ExecuteScalarAsync();
                return System.Text.Json.Nodes.JsonValue.Create(resultValue == System.DBNull.Value ? null : resultValue);
            }
        }

        /// <summary>
        /// Asynchronously retrieves a unique set of categories from the database.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="HashSet{T}"/> of category strings if successful; otherwise, <c>null</c>.</returns>
        public async Task<HashSet<string>?> GetCategoriesAsync()
        {
            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            await npgsqlConnection.OpenAsync();

            return await GetCategoriesAsync(npgsqlConnection);
        }

        /// <summary>
        /// Asynchronously retrieves a unique set of categories from the database using the provided connection.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection instance used to execute the query. This value can be null.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="HashSet{T}"/> of category strings if retrieved successfully; otherwise, null.</returns>
        public async Task<HashSet<string>?> GetCategoriesAsync(NpgsqlConnection? npgsqlConnection)
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

        /// <summary>
        /// Asynchronously retrieves a list of column references filtered by the specified categories.
        /// </summary>
        /// <param name="categories">An optional collection of category names to filter the results. If null, the filtering criteria may be omitted.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <see cref="ColumnReference"/> objects if matches are found; otherwise, null.</returns>
        public async Task<List<ColumnReference>?> GetColumnReferencesByCategoriesAsync(IEnumerable<string>? categories = null)
        {
            return await GetColumnReferencesAsync("category", categories);
        }

        /// <summary>
        /// Asynchronously retrieves a list of column references filtered by the specified categories.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to be used for the database operation.</param>
        /// <param name="categories">An optional collection of category names used to filter the column references.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <see cref="ColumnReference"/> objects if successful; otherwise, null.</returns>
        public async Task<List<ColumnReference>?> GetColumnReferencesByCategoriesAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? categories = null)
        {
            return await GetColumnReferencesAsync(npgsqlConnection, "category", categories);
        }

        /// <summary>
        /// Asynchronously retrieves a list of column references that match the specified names.
        /// </summary>
        /// <param name="names">An optional collection of column names to filter by. If null, the retrieval criteria may vary based on the underlying implementation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <see cref="ColumnReference"/> objects if matches are found; otherwise, null.</returns>
        public async Task<List<ColumnReference>?> GetColumnReferencesByNamesAsync(IEnumerable<string>? names = null)
        {
            return await GetColumnReferencesAsync("name", names);
        }

        /// <summary>
        /// Asynchronously retrieves a list of column references based on the specified names.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection instance used to execute the database query.</param>
        /// <param name="names">An optional collection of column names to filter the search results.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <see cref="ColumnReference"/> objects if matches are found; otherwise, null.</returns>
        public async Task<List<ColumnReference>?> GetColumnReferencesByNamesAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? names = null)
        {
            return await GetColumnReferencesAsync(npgsqlConnection, "name", names);
        }

        /// <summary>
        /// Asynchronously retrieves a list of column references associated with the specified unique identifiers.
        /// </summary>
        /// <param name="uniqueIds">An optional collection of unique identifiers used to filter the column references. If null, the retrieval behavior is determined by the underlying data source.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <see cref="ColumnReference"/> objects if matches are found; otherwise, <see langword="null"/>.</returns>
        public async Task<List<ColumnReference>?> GetColumnReferencesByUniqueIdsAsync(IEnumerable<string>? uniqueIds = null)
        {
            return await GetColumnReferencesAsync("unique_id", uniqueIds);
        }

        /// <summary>
        /// Asynchronously retrieves a list of column references associated with the specified unique identifiers.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection instance used to communicate with the database.</param>
        /// <param name="uniqueIds">An optional collection of unique identifier strings used to filter the column references.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <see cref="ColumnReference"/> objects if matches are found; otherwise, null.</returns>
        public async Task<List<ColumnReference>?> GetColumnReferencesByUniqueIdsAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? uniqueIds = null)
        {
            return await GetColumnReferencesAsync(npgsqlConnection, "unique_id", uniqueIds);
        }

        /// <summary>
        /// Asynchronously retrieves a list of all available column definitions.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <typeparamref name="UColumn"/> objects if columns are found; otherwise, <c>null</c>.</returns>
        public async Task<List<UColumn>?> GetColumnsAsync()
        {
            return await GetColumnsByUniqueIdsAsync();
        }

        /// <summary>
        /// Asynchronously retrieves a list of columns filtered by the specified categories.
        /// </summary>
        /// <param name="categories">An optional collection of category names to filter the columns by. If null, the filtering behavior is determined by the underlying data source.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <typeparamref name="UColumn"/> objects matching the categories, or null if no results are found.</returns>
        public async Task<List<UColumn>?> GetColumnsByCategoriesAsync(IEnumerable<string>? categories = null)
        {
            return await GetColumnsAsync("category", categories);
        }

        /// <summary>
        /// Asynchronously retrieves a list of columns filtered by the specified categories.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection to be used for the database operation.</param>
        /// <param name="categories">An optional collection of category names used to filter the retrieved columns.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <typeparamref name="UColumn"/> objects if successful; otherwise, null.</returns>
        public async Task<List<UColumn>?> GetColumnsByCategoriesAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? categories = null)
        {
            return await GetColumnsAsync(npgsqlConnection, "category", categories);
        }

        /// <summary>
        /// Asynchronously retrieves a list of columns filtered by the specified names.
        /// </summary>
        /// <param name="names">An optional collection of column names to retrieve. If null, the behavior depends on the underlying data source implementation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <typeparamref name="UColumn"/> objects if matches are found; otherwise, null.</returns>
        public async Task<List<UColumn>?> GetColumnsByNamesAsync(IEnumerable<string>? names = null)
        {
            return await GetColumnsAsync("name", names);
        }

        /// <summary>
        /// Asynchronously retrieves a list of columns filtered by the specified names.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection instance used to execute the database query.</param>
        /// <param name="names">An optional collection of column names to retrieve. If null, the filter may be ignored or return no results depending on the underlying implementation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <typeparamref name="UColumn"/> objects if successful; otherwise, null.</returns>
        public async Task<List<UColumn>?> GetColumnsByNamesAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? names = null)
        {
            return await GetColumnsAsync(npgsqlConnection, "name", names);
        }

        /// <summary>
        /// Asynchronously retrieves a list of columns based on the provided unique identifiers.
        /// </summary>
        /// <param name="uniqueIds">An optional collection of unique identifier strings used to filter the columns. If null, the behavior is determined by the underlying data source.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <typeparamref name="UColumn"/> objects matching the specified identifiers, or null if no matches are found.</returns>
        public async Task<List<UColumn>?> GetColumnsByUniqueIdsAsync(IEnumerable<string>? uniqueIds = null)
        {
            return await GetColumnsAsync("unique_id", uniqueIds);
        }

        /// <summary>
        /// Asynchronously retrieves a list of columns based on their unique identifiers.
        /// </summary>
        /// <param name="npgsqlConnection">The Npgsql connection instance used to execute the database query.</param>
        /// <param name="uniqueIds">An optional collection of unique identifier strings used to filter the results.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of <typeparamref name="UColumn"/> objects if found; otherwise, null.</returns>
        public async Task<List<UColumn>?> GetColumnsByUniqueIdsAsync(NpgsqlConnection? npgsqlConnection, IEnumerable<string>? uniqueIds = null)
        {
            return await GetColumnsAsync(npgsqlConnection, "unique_id", uniqueIds);
        }

        /// <summary>
        /// Generates a value distribution histogram for a specific column in a partition.
        /// <para>Resolves partitioning settings dynamically from <see cref="TableConversionOptions"/>.</para>
        /// </summary>
        /// <typeparam name="TColumn">The type of column, which must implement <typeparamref name="UColumn"/>.</typeparam>
        /// <param name="npgsqlConnection">The active database connection instance.</param>
        /// <param name="columnUniqueId">The unique identifier of the column to aggregate.</param>
        /// <param name="bucketCount">The total number of buckets to segment the value range into.</param>
        /// <param name="partitionValue">The partition key value; ignored if partitioning is disabled.</param>
        /// <returns>A task representing the async operation, returning the histogram data as a <see cref="System.Text.Json.Nodes.JsonArray"/>.</returns>
        public async Task<System.Text.Json.Nodes.JsonArray?> GetHistogramSummaryAsync<TColumn>(NpgsqlConnection npgsqlConnection, string columnUniqueId, int bucketCount, object? partitionValue = null)
            where TColumn : UColumn
        {
            System.Collections.Generic.List<UColumn>? existingColumns = await GetColumnsByUniqueIdsAsync(npgsqlConnection, [columnUniqueId]);
            if (existingColumns is null || existingColumns.Count == 0)
            {
                return null;
            }

            string? partitionColumnUniqueId = TableConversionOptions?.PartitioningOptions?.Column?.UniqueId();
            bool hasPartition = !string.IsNullOrEmpty(partitionColumnUniqueId) && partitionValue != null;

            string commandText = $@"
                SELECT width_bucket(""{columnUniqueId}"", min_val, max_val, @bucketCount) as bucket,
                       min(""{columnUniqueId}"") as range_start,
                       max(""{columnUniqueId}"") as range_end,
                       count(*) as count
                FROM ""{TableName}""
                CROSS JOIN (
                    SELECT min(""{columnUniqueId}"") as min_val, max(""{columnUniqueId}"") as max_val
                    FROM ""{TableName}""
                    WHERE {(hasPartition ? $"\"{partitionColumnUniqueId}\" = @partitionValue" : "1=1")}
                ) stats
                WHERE {(hasPartition ? $"\"{partitionColumnUniqueId}\" = @partitionValue" : "1=1")}
                GROUP BY bucket
                ORDER BY bucket;";

            await using NpgsqlCommand npgsqlCommand_Histogram = new NpgsqlCommand(commandText, npgsqlConnection);
            npgsqlCommand_Histogram.Parameters.AddWithValue("bucketCount", bucketCount);
            if (hasPartition)
            {
                npgsqlCommand_Histogram.Parameters.AddWithValue("partitionValue", partitionValue!);
            }

            await using NpgsqlDataReader npgsqlDataReader_Histogram = await npgsqlCommand_Histogram.ExecuteReaderAsync();
            System.Text.Json.Nodes.JsonArray jsonArray_Result = new System.Text.Json.Nodes.JsonArray();
            while (await npgsqlDataReader_Histogram.ReadAsync())
            {
                System.Text.Json.Nodes.JsonObject jsonObject_Bucket = new System.Text.Json.Nodes.JsonObject();
                jsonObject_Bucket["bucket"] = npgsqlDataReader_Histogram.GetInt32(0);
                jsonObject_Bucket["rangeStart"] = npgsqlDataReader_Histogram.IsDBNull(1) ? null : System.Convert.ToDouble(npgsqlDataReader_Histogram.GetValue(1));
                jsonObject_Bucket["rangeEnd"] = npgsqlDataReader_Histogram.IsDBNull(2) ? null : System.Convert.ToDouble(npgsqlDataReader_Histogram.GetValue(2));
                jsonObject_Bucket["count"] = npgsqlDataReader_Histogram.GetInt64(3);
                jsonArray_Result.Add(jsonObject_Bucket);
            }
            return jsonArray_Result;
        }

        /// <summary>
        /// Asynchronously retrieves a collection of unique values associated with the specified identifier.
        /// </summary>
        /// <typeparam name="T">The type of the elements contained in the returned collection.</typeparam>
        /// <param name="uniqueId">The unique identifier used to query for the values; may be null.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains an enumerable collection of nullable values of type <typeparamref name="T"/>, or null if the operation cannot be completed or no data is found.</returns>
        public async Task<IEnumerable<T?>?> GetUniqueValuesAsync<T>(string? uniqueId)
        {
            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);

            if (npgsqlConnection is null)
            {
                return null;
            }

            await npgsqlConnection.OpenAsync();

            return await GetUniqueValuesAsync<T>(npgsqlConnection, uniqueId);
        }

        /// <summary>
        /// Retrieves a distinct list of values from a specified column in the database.
        /// </summary>
        /// <typeparam name="T">The target type for the retrieved values.</typeparam>
        /// <param name="npgsqlConnection">The active PostgreSQL connection instance.</param>
        /// <param name="uniqueId">The name of the database column to query.</param>
        /// <returns>An enumerable containing unique values from the column, or null if input is invalid.</returns>
        public async Task<IEnumerable<T?>?> GetUniqueValuesAsync<T>(NpgsqlConnection? npgsqlConnection, string? uniqueId)
        {
            // 1. Basic input validation: Check if connection exists and uniqueId (column name) is provided.
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(uniqueId))
            {
                return null;
            }

            // 2. Prepare the SQL query.
            // SECURITY WARNING: Since column names cannot be parameterized, ensure 'uniqueId'
            // is validated against a whitelist to prevent SQL Injection.
            string commandQuery = $@"
                SELECT DISTINCT {uniqueId}
                FROM {TableName}
                WHERE {uniqueId} IS NOT NULL
                ORDER BY {uniqueId}";

            // 3. Initialize result set. Using HashSet to ensure uniqueness in memory
            // (complementary to the SQL DISTINCT clause).
            HashSet<T?> result = [];

            // 4. Create and execute the Npgsql command.
            using NpgsqlCommand npgsqlCommand = new(commandQuery, npgsqlConnection);

            // 5. Open a data reader to stream results from the database.
            using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            // 6. Iterate through the records returned by PostgreSQL.
            while (await npgsqlDataReader.ReadAsync())
            {
                // Check for database NULL or try to convert the raw object to the generic type T.
                if (npgsqlDataReader.IsDBNull(0) || !Core.Query.TryConvert(npgsqlDataReader.GetValue(0), out T? value))
                {
                    // If value is NULL or conversion fails, add the default value for type T.
                    result.Add(default);
                }
                else
                {
                    // Add the successfully converted value to the result set.
                    result.Add(value);
                }
            }

            return result;
        }

        /// <summary>
        /// Asynchronously pulls specific data from the specified table based on a unique column values.
        /// </summary>
        public async Task<bool> PullAsync<TObject, TColumn, TRow>(NpgsqlConnection? npgsqlConnection, Table<TColumn, TRow>? table, string columnUniqueId, IEnumerable<TObject>? values) where TColumn : UColumn where TRow : IRow<TRow>
        {
            if (table is null || npgsqlConnection is null || string.IsNullOrWhiteSpace(columnUniqueId) || values is null || !values.Any())
            {
                return false;
            }

            if (values is null || !values.Any())
            {
                return false;
            }

            if (table.Columns is not IEnumerable<TColumn> columns || !columns.Any())
            {
                return false;
            }

            List<UColumn>? columns_Existing = await GetColumnsByUniqueIdsAsync([columnUniqueId]);
            if (columns_Existing is null || columns_Existing.Count == 0)
            {
                return false;
            }

            UColumn? column_Existing = columns_Existing.Find(x => x?.UniqueId() == columnUniqueId);
            if (column_Existing?.NpgsqlDbType() is not NpgsqlDbType npgsqlDbType)
            {
                return false;
            }

            // Validate and collect all provided values
            List<object> validValues = [];
            foreach (TObject value in values)
            {
                if (column_Existing.TryGetValidValue(value, out object? value_Temp) && value_Temp is not null)
                {
                    validValues.Add(value_Temp);
                }
            }

            // If none of the provided values were valid, there is nothing to query
            if (validValues.Count == 0)
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
            string paramName = "targetValues";
            string commandText;
            NpgsqlParameter npgsqlParameter;

            // Dynamically build the query based on the count of valid values
            if (validValues.Count == 1)
            {
                commandText = $"SELECT {string.Join(", ", quotedColumns)} FROM \"{TableName}\" WHERE \"{columnUniqueId}\" = @{paramName}";
                npgsqlParameter = new NpgsqlParameter(paramName, npgsqlDbType)
                {
                    Value = validValues[0]
                };
            }
            else
            {
                commandText = $"SELECT {string.Join(", ", quotedColumns)} FROM \"{TableName}\" WHERE \"{columnUniqueId}\" = ANY(@{paramName})";
                // Combine Array flag with the specific NpgsqlDbType
                npgsqlParameter = new NpgsqlParameter(paramName, NpgsqlDbType.Array | npgsqlDbType)
                {
                    Value = validValues.ToArray()
                };
            }

            // Setup Primary Keys dictionary for proper merging in ReadAsync
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

            // Map existing rows to avoid duplicates and update them properly
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

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            npgsqlCommand.Parameters.Add(npgsqlParameter);

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            if (!await ReadAsync(npgsqlDataReader, table, dictionary, dictionary_PrimaryKey, existingRowsMap))
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Asynchronously pulls specific data from the specified table based on a unique column value.
        /// </summary>
        public async Task<bool> PullAsync<TColumn, TRow>(NpgsqlConnection? npgsqlConnection, Table<TColumn, TRow>? table, string columnUniqueId, object? value) where TColumn : UColumn where TRow : IRow<TRow>
        {
            return await PullAsync(npgsqlConnection, table, columnUniqueId, [value]);
        }

        /// <summary>
        /// Asynchronously pulls specific data from the specified table based on a unique column value.
        /// </summary>
        public async Task<bool> PullAsync<TColumn, TRow>(Table<TColumn, TRow>? table, string columnUniqueId, object? value) where TColumn : UColumn where TRow : IRow<TRow>
        {
            return await PullAsync(table, columnUniqueId, [value]);
        }

        /// <summary>
        /// Asynchronously pulls specific data from the specified table based on a unique column values.
        /// </summary>
        public async Task<bool> PullAsync<TObject, TColumn, TRow>(Table<TColumn, TRow>? table, string columnUniqueId, IEnumerable<TObject>? values) where TColumn : UColumn where TRow : IRow<TRow>
        {
            if (values is null || !values.Any())
            {
                return false;
            }

            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            await npgsqlConnection.OpenAsync();

            return await PullAsync(npgsqlConnection, table, columnUniqueId, values);
        }

        /// <summary>
        /// Asynchronously pulls data from the specified table using the provided Npgsql connection in batches.
        /// </summary>
        /// <typeparam name="TColumn">The type of the column, which must derive from <typeparamref name="UColumn"/>.</typeparam>
        /// <typeparam name="TRow">The type of the row, which must implement <see cref="IRow{TRow}"/>.</typeparam>
        /// <param name="npgsqlConnection">The Npgsql database connection to use for the operation. May be null.</param>
        /// <param name="table">The table instance from which data is being pulled. May be null.</param>
        /// <param name="batchSize">The number of records to process per batch. Defaults to 1000.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is <see langword="true"/> if the pull operation completed successfully; otherwise, <see langword="false"/>.</returns>
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

        /// <summary>
        /// Asynchronously pulls data from the database for the specified table using a defined batch size.
        /// </summary>
        /// <typeparam name="TColumn">The type of the column, which must derive from <typeparamref name="UColumn"/>.</typeparam>
        /// <typeparam name="TRow">The type of the row, which must implement <see cref="IRow{TRow}"/>.</typeparam>
        /// <param name="table">The table instance to pull data for. This value can be null.</param>
        /// <param name="batchSize">The number of records to retrieve in each batch. The default value is 1000.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the data was pulled successfully; otherwise, <see langword="false"/>.</returns>
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

        /// <summary>
        /// Asynchronously pulls a chunk of data from a table using keyset (cursor-based) pagination.
        /// <para>Resolves partitioning settings dynamically from <see cref="TableConversionOptions"/>.</para>
        /// </summary>
        /// <typeparam name="TColumn">The type of column, which must implement <typeparamref name="UColumn"/>.</typeparam>
        /// <typeparam name="TRow">The type of row, which must implement <see cref="IRow{TRow}"/>.</typeparam>
        /// <param name="npgsqlConnection">The active database connection instance.</param>
        /// <param name="table">The table instance to populate with page data.</param>
        /// <param name="seekColumnUniqueId">The unique identifier of the column to sort and seek by.</param>
        /// <param name="lastSeekValue">The seek column value of the last row from the previous page.</param>
        /// <param name="pageSize">The maximum number of records to retrieve in this page.</param>
        /// <param name="partitionValue">The partition key value; ignored if partitioning is disabled.</param>
        /// <returns>A task representing the asynchronous operation, returning true if successful; otherwise, false.</returns>
        public async Task<bool> PullAsync<TColumn, TRow>(NpgsqlConnection npgsqlConnection, Table<TColumn, TRow>? table, string seekColumnUniqueId, object? lastSeekValue, int pageSize, object? partitionValue = null)
            where TColumn : UColumn
            where TRow : IRow<TRow>
        {
            if (table is null || npgsqlConnection is null || string.IsNullOrWhiteSpace(seekColumnUniqueId))
            {
                return false;
            }

            if (table.Columns is not System.Collections.Generic.IEnumerable<TColumn> columns || !columns.Any())
            {
                return false;
            }

            System.Collections.Generic.Dictionary<string, TColumn> dictionary_Columns = new System.Collections.Generic.Dictionary<string, TColumn>();
            foreach (TColumn column in columns)
            {
                if (column.UniqueId() is not string uniqueId || string.IsNullOrWhiteSpace(uniqueId))
                {
                    continue;
                }

                dictionary_Columns[uniqueId] = column;
            }

            // Resolve partitioning column from conversion options
            string? partitionColumnUniqueId = TableConversionOptions?.PartitioningOptions?.Column?.UniqueId();
            bool hasPartition = !string.IsNullOrEmpty(partitionColumnUniqueId) && partitionValue != null;

            if ((hasPartition && !dictionary_Columns.ContainsKey(partitionColumnUniqueId!)) || !dictionary_Columns.ContainsKey(seekColumnUniqueId))
            {
                return false;
            }

            System.Collections.Generic.IEnumerable<string> quotedColumns = dictionary_Columns.Keys.Select(x => $"\"{x}\"");

            // Build conditional where clauses to handle optional partitioning
            System.Collections.Generic.List<string> whereClauses = new System.Collections.Generic.List<string>();
            if (hasPartition)
            {
                whereClauses.Add($"\"{partitionColumnUniqueId}\" = @partitionValue");
            }
            if (lastSeekValue != null)
            {
                whereClauses.Add($"\"{seekColumnUniqueId}\" > @lastSeekValue");
            }

            string whereQuery = whereClauses.Count > 0 ? $"WHERE {string.Join(" AND ", whereClauses)}" : string.Empty;

            string commandText = $@"
                SELECT {string.Join(", ", quotedColumns)}
                FROM ""{TableName}""
                {whereQuery}
                ORDER BY ""{seekColumnUniqueId}"" ASC
                LIMIT @pageSize";

            await using NpgsqlCommand npgsqlCommand_Select = new NpgsqlCommand(commandText, npgsqlConnection);
            npgsqlCommand_Select.Parameters.AddWithValue("pageSize", pageSize);
            if (hasPartition)
            {
                npgsqlCommand_Select.Parameters.AddWithValue("partitionValue", partitionValue!);
            }
            if (lastSeekValue != null)
            {
                npgsqlCommand_Select.Parameters.AddWithValue("lastSeekValue", lastSeekValue);
            }

            System.Collections.Generic.Dictionary<string, TColumn> dictionary_PrimaryKey = new System.Collections.Generic.Dictionary<string, TColumn>();
            if (TableConversionOptions?.PrimaryKeyColumns is System.Collections.Generic.List<UColumn> columns_PrimaryKey && columns_PrimaryKey.Count != 0)
            {
                foreach (UColumn column_PrimaryKey in columns_PrimaryKey)
                {
                    if (column_PrimaryKey.UniqueId() is not string uniqueId || string.IsNullOrWhiteSpace(uniqueId))
                    {
                        continue;
                    }

                    if (!dictionary_Columns.TryGetValue(uniqueId, out TColumn? column))
                    {
                        continue;
                    }

                    dictionary_PrimaryKey[uniqueId] = column;
                }
            }

            System.Collections.Generic.Dictionary<string, TRow> existingRows = new System.Collections.Generic.Dictionary<string, TRow>();
            await using NpgsqlDataReader npgsqlDataReader_Select = await npgsqlCommand_Select.ExecuteReaderAsync();

            return await ReadAsync(npgsqlDataReader_Select, table, dictionary_Columns, dictionary_PrimaryKey, existingRows);
        }

        /// <summary>
        /// Asynchronously pushes the contents of the specified table to the database using batch processing.
        /// </summary>
        /// <typeparam name="TColumn">The type of the column, which must derive from <typeparamref name="UColumn"/>.</typeparam>
        /// <typeparam name="TRow">The type of the row, which must implement <see cref="IRow{TRow}"/>.</typeparam>
        /// <param name="table">The table instance containing the data to be pushed. This value can be null.</param>
        /// <param name="batchSize">The number of records to process in each batch. The default value is 1000.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <c>true</c> if the push operation was successful; otherwise, <c>false</c>.</returns>
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

        /// <summary>
        /// Asynchronously pushes data from the specified table to the database using the provided Npgsql connection.
        /// </summary>
        /// <typeparam name="TColumn">The type of the column, which must derive from <typeparamref name="UColumn"/>.</typeparam>
        /// <typeparam name="TRow">The type of the row, which must implement <see cref="IRow{TRow}"/>.</typeparam>
        /// <param name="npgsqlConnection">The Npgsql connection instance used to communicate with the database. May be null.</param>
        /// <param name="table">The table containing the data to be pushed. May be null.</param>
        /// <param name="batchSize">The number of records to process per batch. Defaults to 1000.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is <see langword="true"/> if the data was successfully pushed; otherwise, <see langword="false"/>.</returns>
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
                    //values[keyValuePair.Value.UniqueId()!] = npgsqlDataReader[keyValuePair.Key];
                    values[keyValuePair.Value.Name!] = npgsqlDataReader[keyValuePair.Key];
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

        private async Task<List<ColumnReference>?> GetColumnReferencesAsync(NpgsqlConnection? npgsqlConnection, string columnName, IEnumerable<string>? values = null)
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

        private async Task<List<ColumnReference>?> GetColumnReferencesAsync(string columnName, IEnumerable<string>? values = null)
        {
            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);

            if (npgsqlConnection is null)
            {
                return null;
            }

            await npgsqlConnection.OpenAsync();

            return await GetColumnReferencesAsync(columnName, values);
        }

        private async Task<List<UColumn>?> GetColumnsAsync(string columnName, IEnumerable<string>? values = null)
        {
            await using NpgsqlConnection? npgsqlConnection = PostgreSQL.Create.NpgsqlConnection(ConnectionData);

            if (npgsqlConnection is null)
            {
                return null;
            }

            await npgsqlConnection.OpenAsync();

            return await GetColumnsAsync(npgsqlConnection, columnName, values);
        }

        private async Task<List<UColumn>?> GetColumnsAsync(NpgsqlConnection? npgsqlConnection, string columnName, IEnumerable<string>? values = null)
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