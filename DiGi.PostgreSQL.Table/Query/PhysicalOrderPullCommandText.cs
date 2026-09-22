using System.Collections.Generic;
using System.Linq;

namespace DiGi.PostgreSQL.Table
{
    public static partial class Query
    {
        /// <summary>
        /// Builds the statement that reads one window of a partition in physical order, the page query of <c>TablePostgreSQLConverter.PullByPhysicalOrderAsync</c>.
        /// <para>The window is bounded on <b>both</b> sides - <c>ctid &gt; @lowerPosition AND ctid &lt; @upperPosition</c> - because that is what the planner serves with a TID Range Scan, reading only the window's blocks. A lower bound alone covers the rest of the partition, and the planner then answers <c>ORDER BY ctid LIMIT n</c> with a sequential scan and a top-N sort of everything after the bound, on every page (measured on PostgreSQL 18: 7 143 buffers per page against 150 for a bounded window).</para>
        /// <para>The rows come back ordered by <c>ctid</c> and capped at <c>@pageSize</c>, with the position of each row as text in the extra column named by <see cref="Constants.ColumnName.PhysicalPosition"/>. Parameters: <c>@partitionValue</c>, <c>@lowerPosition</c> and <c>@upperPosition</c> (tid text such as <c>(12,0)</c>) and <c>@pageSize</c>.</para>
        /// <para>Identifiers are quoted, never parameterised; callers pass the table and column unique ids the converter already holds, not caller-supplied text.</para>
        /// </summary>
        /// <param name="tableName">The name of the partitioned table. This value can be null.</param>
        /// <param name="columnUniqueIds">The unique ids of the columns to read. This value can be null.</param>
        /// <param name="partitionColumnUniqueId">The unique id of the partitioning column. This value can be null.</param>
        /// <returns>The statement, or <see langword="null"/> when the table name, the partitioning column or every column id is missing.</returns>
        public static string? PhysicalOrderPullCommandText(string? tableName, IEnumerable<string>? columnUniqueIds, string? partitionColumnUniqueId)
        {
            if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(partitionColumnUniqueId) || columnUniqueIds is null)
            {
                return null;
            }

            List<string> quotedColumns = [.. columnUniqueIds.Where(x => !string.IsNullOrWhiteSpace(x)).Distinct().Select(x => $"\"{x}\"")];
            if (quotedColumns.Count == 0)
            {
                return null;
            }

            return $@"
                SELECT {string.Join(", ", quotedColumns)}, ctid::text AS ""{Constants.ColumnName.PhysicalPosition}""
                FROM ""{tableName}""
                WHERE ""{partitionColumnUniqueId}"" = @partitionValue AND ctid > @lowerPosition::tid AND ctid < @upperPosition::tid
                ORDER BY ctid ASC
                LIMIT @pageSize";
        }
    }
}
