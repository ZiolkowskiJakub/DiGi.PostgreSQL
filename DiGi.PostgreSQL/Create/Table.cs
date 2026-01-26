using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        public async static Task<bool> Table_Types(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return false;
            }

            string commandText = @"
                CREATE TABLE IF NOT EXISTS types (
                    id          smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    full_name        text NOT NULL UNIQUE,
                    created_at  timestamptz DEFAULT now()
                );";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

            await npgsqlCommand.ExecuteNonQueryAsync();

            return true;
        }

        public async static Task<bool> Table_Objects(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return false;
            }

            string commandText;

            commandText = @"
                CREATE TABLE IF NOT EXISTS objects (
                    id             bigint GENERATED ALWAYS AS IDENTITY,
                    type_id smallint NOT NULL REFERENCES types(id),
                    unique_id    text,
                    data           jsonb NOT NULL,
                    created_at     timestamptz DEFAULT now(),
                    PRIMARY KEY (id, type_id)
                ) PARTITION BY LIST (type_id);
                ";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

            await npgsqlCommand.ExecuteNonQueryAsync();

            commandText = @"
                -- 1. This UNIQUE index acts as the constraint for ON CONFLICT
                CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_unique_pair
                    ON objects (type_id, unique_id);

                -- 2. GIN index for JSON performance
                CREATE INDEX IF NOT EXISTS idx_objects_data_gin
                    ON objects USING GIN (data);
            ";

            return true;
        }

        public async static Task<bool> Table_Objects_Partition(this NpgsqlConnection? npgsqlConnection, short typeId)
        {
            if (npgsqlConnection is null)
            {
                return false;
            }

            string commandText = $@"
                CREATE TABLE IF NOT EXISTS objects_type_{typeId} PARTITION OF objects
                    FOR VALUES IN ({typeId});
                ";

            await using NpgsqlCommand npgsqlCommand = new(commandText, npgsqlConnection);

            await npgsqlCommand.ExecuteNonQueryAsync();

            return true;
        }
    }
}