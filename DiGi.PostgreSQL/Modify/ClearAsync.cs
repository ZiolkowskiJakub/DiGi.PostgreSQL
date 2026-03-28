using Npgsql;
using System.Threading;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<bool> ClearAsync(NpgsqlConnection? npgsqlConnection, string tableName, CancellationToken cancellationToken = default)
        {
            // Use TRUNCATE for speed, or DELETE for transactional safety
            string commandText = $"TRUNCATE TABLE {tableName} RESTART IDENTITY CASCADE;";

            using NpgsqlCommand command = new(commandText, npgsqlConnection);
            try
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
                return true;
            }
            catch
            {
                // Handle logging here
                return false;
            }
        }
    }
}