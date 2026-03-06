using DiGi.PostgreSQL.Enums;
using Npgsql;
using System;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
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
        /// Creates a lookup table for object types to optimize storage and filtering.
        /// Includes a timestamp for auditing when the type was first introduced.
        /// </summary>
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