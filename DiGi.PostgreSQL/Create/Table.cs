using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Create
    {
        public static async Task<bool> Table_Types(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return false;
            }

            const string commandText = @"
                CREATE TABLE IF NOT EXISTS types (
                    id          smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    full_name        text NOT NULL UNIQUE,
                    created_at  timestamptz DEFAULT now()
                );";

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

        public static async Task<bool> Table_Objects(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null)
            {
                return false;
            }

            const string commandText = @"
                CREATE TABLE IF NOT EXISTS objects (
                    id         bigint GENERATED ALWAYS AS IDENTITY,
                    type_id    smallint NOT NULL REFERENCES types(id),
                    unique_id  text,
                    data       jsonb NOT NULL,
                    created_at timestamptz DEFAULT now(),
                    PRIMARY KEY (id, type_id)
                ) PARTITION BY LIST (type_id);

                CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_unique_pair
                    ON objects (type_id, unique_id);

                CREATE INDEX IF NOT EXISTS idx_objects_data_gin
                    ON objects USING GIN (data);";

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

        public static async Task<bool> Table_Objects_Partition(this NpgsqlConnection? npgsqlConnection, short typeId)
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