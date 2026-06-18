using DiGi.PostgreSQL.Enums;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        /// <summary>
        /// Asynchronously creates the main objects table for a specific data type, including optional GIN indexing and type column referencing.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance used to execute the command.</param>
        /// <param name="dataType">The data type that determines the table name and storage format.</param>
        /// <param name="useGIN">A value indicating whether a GIN index should be created for JSON data types.</param>
        /// <param name="includeType">A value indicating whether to include a reference column to the types lookup table.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the table was created successfully; otherwise, false.</returns>
        public static async Task<bool> TableAsync_Objects(this NpgsqlConnection? npgsqlConnection, DataType dataType, bool useGIN = false, bool includeType = false)
        {
            if (npgsqlConnection is null || dataType == DataType.Undefined)
            {
                return false;
            }

            string typeName = "jsonb";
            if (dataType == DataType.Binary || dataType == DataType.Archive)
            {
                typeName = "bytea";
            }

            // Optional column referencing the 'types' lookup table
            string typeColumnSql = includeType
                ? "type_id smallint NOT NULL REFERENCES types(id),"
                : string.Empty;

            // We adjust the unique index based on whether we include types or not.
            // If includeType is true, uniqueness is defined by partition + type + unique_id.
            string uniqueIndexColumns = includeType
                ? "partition_id, type_id, unique_id"
                : "partition_id, unique_id";

            string typeIndexSql = includeType
                ? $@"CREATE INDEX IF NOT EXISTS idx_objects_{(int)dataType}_type
             ON objects_{(int)dataType} (type_id);"
                : string.Empty;

            string commandText = $@"
                CREATE TABLE IF NOT EXISTS objects_{(int)dataType} (
                    id             bigint GENERATED ALWAYS AS IDENTITY,
                    partition_id   smallint NOT NULL REFERENCES partitions(id),
                    {typeColumnSql}
                    unique_id      text NOT NULL,
                    data           {typeName} NOT NULL,
                    created_at     timestamptz DEFAULT now(),
                    -- Primary Key must contain partition key (partition_id)
                    PRIMARY KEY (id, partition_id)
                ) PARTITION BY LIST (partition_id);

                -- Adjusted Unique Index to account for the 'type_id' if requested
                CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_{(int)dataType}_unique_pair
                    ON objects_{(int)dataType} ({uniqueIndexColumns});

                {typeIndexSql}";

            if (useGIN && dataType == DataType.Json)
            {
                commandText += $@"
                    CREATE INDEX IF NOT EXISTS idx_objects_{(int)dataType}_data_gin
                        ON objects_{(int)dataType} USING GIN (data)
                        WHERE data IS NOT NULL;";
            }

            try
            {
                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                await npgsqlCommand.ExecuteNonQueryAsync();
                return true;
            }
            catch (NpgsqlException ex)
            {
                Console.WriteLine($"Postgres Error (Table_Objects): {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Asynchronously creates a specific partition for the objects table based on the provided data type and partition identifier.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance used to execute the command.</param>
        /// <param name="dataType">The data type associated with the parent objects table.</param>
        /// <param name="partitionId">The unique identifier for the partition being created.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the partition was created successfully; otherwise, false.</returns>
        public static async Task<bool> TableAsync_Objects_Partition(this NpgsqlConnection? npgsqlConnection, DataType dataType, short partitionId)
        {
            if (npgsqlConnection is null || dataType == DataType.Undefined)
            {
                return false;
            }

            string commandText = $@"
                CREATE TABLE IF NOT EXISTS objects_{(int)dataType}_{partitionId} PARTITION OF objects_{(int)dataType}
                    FOR VALUES IN ({partitionId});
                ";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

            await npgsqlCommand.ExecuteNonQueryAsync();

            return true;
        }

        /// <summary>
        /// Asynchronously creates a partition for a specified parent table based on a collection of provided values.
        /// </summary>
        /// <typeparam name="T">The type of the elements in the values collection.</typeparam>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance used to execute the command.</param>
        /// <param name="tableName">The name of the parent table that is being partitioned.</param>
        /// <param name="partitionNameSufix">The suffix to be appended to the parent table name to create the partition table name.</param>
        /// <param name="values">A collection of values for which this partition will be responsible.</param>
        /// <param name="conversionFunc">An optional function to convert each value of type T into a string representation for the SQL command.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the partition was created successfully; otherwise, false.</returns>
        public static async Task<bool> TableAsync_Partition<T>(this NpgsqlConnection? npgsqlConnection, string tableName, string partitionNameSufix, IEnumerable<T> values, Func<T, string>? conversionFunc = null)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(partitionNameSufix) || values is null || !values.Any())
            {
                return false;
            }

            HashSet<string> formattedValues = [];
            foreach (T value in values)
            {
                string? rawValue = conversionFunc is null ? value?.ToString() : conversionFunc.Invoke(value);
                if (string.IsNullOrWhiteSpace(rawValue))
                {
                    continue;
                }

                string escapedValue = rawValue.Replace("'", "''");
                formattedValues.Add($"'{escapedValue}'");
            }

            if (formattedValues.Count == 0)
            {
                return false;
            }

            NpgsqlCommandBuilder npgsqlCommandBuilder = new();

            string safeParentTable = npgsqlCommandBuilder.QuoteIdentifier(tableName);
            string safePartitionTable = npgsqlCommandBuilder.QuoteIdentifier($"{tableName}_{partitionNameSufix}");
            string valuesList = string.Join(", ", formattedValues);

            string commandText = $@"
                CREATE TABLE IF NOT EXISTS {safePartitionTable}
                PARTITION OF {safeParentTable}
                FOR VALUES IN ({valuesList});";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
            await npgsqlCommand.ExecuteNonQueryAsync();

            return true;
        }

        /// <summary>
        /// Asynchronously creates a default partition for the specified parent table to handle any values not matched by other partitions.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance used to execute the command.</param>
        /// <param name="tableName">The name of the parent table for which the default partition is created.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the default partition was created successfully; otherwise, false.</returns>
        public static async Task<bool> TableAsync_Partition_Default(this NpgsqlConnection? npgsqlConnection, string tableName)
        {
            if (npgsqlConnection is null || string.IsNullOrWhiteSpace(tableName))
            {
                return false;
            }

            NpgsqlCommandBuilder commandBuilder = new();

            // Secure the identifiers for PostgreSQL
            string safeParentTable = commandBuilder.QuoteIdentifier(tableName);
            string safeDefaultPartitionTable = commandBuilder.QuoteIdentifier($"{tableName}_default");

            // Construct the DDL command using DEFAULT keyword
            string commandText = $@"
            CREATE TABLE IF NOT EXISTS {safeDefaultPartitionTable}
            PARTITION OF {safeParentTable}
            DEFAULT;";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

            try
            {
                await npgsqlCommand.ExecuteNonQueryAsync();
                return true;
            }
            catch (PostgresException)
            {
                // Handle cases where a default partition might conflict or fail
                return false;
            }
        }

        /// <summary>
        /// Asynchronously creates the partitions lookup table used to manage and track data partitioning.
        /// </summary>
        /// <param name="npgsqlConnection">The PostgreSQL connection instance used to execute the command.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the partitions table was created successfully; otherwise, false.</returns>
        public static async Task<bool> TableAsync_Partitions(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return false;
            }

            // Using smallint to store the underlying value of the C# enum
            const string commandText = @"
                CREATE TABLE IF NOT EXISTS partitions (
                    id           smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    name         text NOT NULL UNIQUE,
                    data_type    smallint NOT NULL,
                    created_at   timestamptz DEFAULT now()
                );";

            try
            {
                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

                await npgsqlCommand.ExecuteNonQueryAsync();
                return true;
            }
            catch (NpgsqlException ex)
            {
                // For production plugins, consider logging to a specific file or BIM platform console
                Console.WriteLine($"Postgres Error: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Asynchronously creates the 'types' lookup table in the PostgreSQL database to optimize storage and filtering, including a timestamp for auditing when the type was first introduced.
        /// </summary>
        /// <param name="npgsqlConnection">The <see cref="Npgsql.NpgsqlConnection"/> instance used to execute the create table command.</param>
        /// <returns>A task that represents the asynchronous operation. The task result is true if the table was created successfully or already exists; otherwise, false.</returns>
        public static async Task<bool> TableAsync_Types(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return false;
            }

            // Using timestamptz to ensure consistent time tracking across different time zones
            const string commandText = @"
                CREATE TABLE IF NOT EXISTS types (
                    id          smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    name        text NOT NULL UNIQUE,
                    created_at  timestamptz DEFAULT now()
                );";

            try
            {
                // Explicitly using NpgsqlCommand type instead of implicit typing
                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

                await npgsqlCommand.ExecuteNonQueryAsync();
                return true;
            }
            catch (NpgsqlException ex)
            {
                // Logging the error to console - in ASP.NET Core we will later replace this with ILogger
                Console.WriteLine($"Postgres Error (Table_Types): {ex.Message}");
                return false;
            }
        }
    }
}
