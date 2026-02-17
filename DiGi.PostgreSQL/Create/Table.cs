using DiGi.PostgreSQL.Enums;
using Npgsql;
using System;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        public static async Task<bool> Table_Partitions(this NpgsqlConnection? npgsqlConnection)
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

        public static async Task<bool> Table_Objects(this NpgsqlConnection? npgsqlConnection, DataType dataType)
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

            string commandText = $@"
                CREATE TABLE IF NOT EXISTS objects_{(int)dataType} (
                    id         bigint GENERATED ALWAYS AS IDENTITY,
                    partition_id    smallint NOT NULL REFERENCES partitions(id),
                    unique_id  text,
                    data       {typeName} NOT NULL,
                    created_at timestamptz DEFAULT now(),
                    PRIMARY KEY (id, partition_id)
                ) PARTITION BY LIST (partition_id);

                CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_unique_pair
                    ON objects_{(int)dataType} (partition_id, unique_id);";

            if (dataType == DataType.Json)
            {
                commandText += $@"
                    CREATE INDEX IF NOT EXISTS idx_objects_data_gin
                        ON objects_{(int)dataType} USING GIN (data)
                        WHERE data IS NOT NULL;";
            }

            try
            {
                await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);
                await npgsqlCommand.ExecuteNonQueryAsync();
                return true; // If we reach here, the SQL command was successful
            }
            catch
            {
                // Handle specific DB errors (permissions, connection loss, etc.)
                return false;
            }
        }

        public static async Task<bool> Table_Objects_Partition(this NpgsqlConnection? npgsqlConnection, DataType dataType, short partitionId)
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
    }
}