using Npgsql;
using System.Data;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
        /// <summary>
        /// Determines whether the server behind the connection can read a partition in physical order, the read <c>TablePostgreSQLConverter.PullByPhysicalOrderAsync</c> performs.
        /// <para>The read bounds each page by a window of heap positions (<c>ctid</c>), which only a <b>TID Range Scan</b> serves block by block. PostgreSQL added that scan in version 14; on an older server the same condition is a filter over a sequential scan of the whole partition, run once per page - quadratic over a walk. Callers use this check to fall back to a keyset read instead of inferring the reason from a <see langword="null"/> result.</para>
        /// <para>The version is the one Npgsql recorded when the connection opened (<see cref="NpgsqlConnection.PostgreSqlVersion"/>), so the check costs no round trip. It requires an open connection.</para>
        /// </summary>
        /// <param name="npgsqlConnection">The open connection to the server. This value can be null.</param>
        /// <returns><see langword="true"/> when the connection is open and the server is PostgreSQL 14 or later; otherwise <see langword="false"/>, including for a null or closed connection.</returns>
        public static bool IsPhysicalOrderSupported(this NpgsqlConnection? npgsqlConnection)
        {
            if (npgsqlConnection is null || npgsqlConnection.State != ConnectionState.Open)
            {
                return false;
            }

            return npgsqlConnection.PostgreSqlVersion.Major >= 14;
        }
    }
}
