using Npgsql;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<bool> Clear(NpgsqlConnection? npgsqlConnection, string tableName)
        {
            // Use TRUNCATE for speed, or DELETE for transactional safety
            string commandText = $"TRUNCATE TABLE {tableName} RESTART IDENTITY CASCADE;";

            using NpgsqlCommand command = new(commandText, npgsqlConnection);
            try
            {
                await command.ExecuteNonQueryAsync();
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