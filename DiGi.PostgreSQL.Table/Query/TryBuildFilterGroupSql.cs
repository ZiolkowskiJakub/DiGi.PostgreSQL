using DiGi.Core.IO.Table.Interfaces;
using DiGi.PostgreSQL.Table.Classes;
using DiGi.PostgreSQL.Table.Enums;
using Npgsql;
using System.Collections.Generic;
using System.Text;

namespace DiGi.PostgreSQL.Table
{
    /// <summary>
    /// Static partial class containing query extension methods for database operations.
    /// </summary>
    public static partial class Query
    {
        /// <summary>
        /// Recursively builds the SQL query condition and parameters from the specified <see cref="FilterGroup"/>.
        /// </summary>
        /// <typeparam name="UColumn">The base column type deriving from <see cref="IColumn"/>.</typeparam>
        /// <param name="filterGroup">The filter group instance to build SQL for.</param>
        /// <param name="existingColumns">The list of valid whitelisted database columns.</param>
        /// <param name="stringBuilder_Sql">The string builder to append the resulting SQL condition to.</param>
        /// <param name="npgsqlParameterCollection">The parameter collection to bind Npgsql parameters to.</param>
        /// <param name="parameterIndex">A reference counter for unique query parameter names.</param>
        /// <returns>True if the SQL condition was successfully built; otherwise, false.</returns>
        public static bool TryBuildFilterGroupSql<UColumn>(
            this FilterGroup? filterGroup,
            List<UColumn> existingColumns,
            StringBuilder stringBuilder_Sql,
            NpgsqlParameterCollection npgsqlParameterCollection,
            ref int parameterIndex)
            where UColumn : IColumn
        {
            if (filterGroup is null)
            {
                return true;
            }

            List<string> conditions = [];

            if (filterGroup.FilterConditions is not null)
            {
                foreach (FilterCondition filterCondition in filterGroup.FilterConditions)
                {
                    if (filterCondition is null || string.IsNullOrWhiteSpace(filterCondition.ColumnUniqueId))
                    {
                        continue;
                    }

                    UColumn? column = existingColumns.Find(x => x?.UniqueId() == filterCondition.ColumnUniqueId);
                    if (column is null)
                    {
                        return false; // Whitelist violation!
                    }

                    if (column.NpgsqlDbType() is not NpgsqlTypes.NpgsqlDbType npgsqlDbType)
                    {
                        return false;
                    }

                    switch (filterCondition.FilterOperator)
                    {
                        case FilterOperator.IsNull:
                            conditions.Add($"\"{filterCondition.ColumnUniqueId}\" IS NULL");
                            break;

                        case FilterOperator.IsNotNull:
                            conditions.Add($"\"{filterCondition.ColumnUniqueId}\" IS NOT NULL");
                            break;

                        case FilterOperator.Contains:
                            if (filterCondition.Value is not null)
                            {
                                if (column.TryGetValidValue(filterCondition.Value, out object? validValue_Contains) && validValue_Contains is not null)
                                {
                                    string paramName = $"filterParam_{parameterIndex}";
                                    parameterIndex++;
                                    conditions.Add($"\"{filterCondition.ColumnUniqueId}\" ILIKE @{paramName}");
                                    npgsqlParameterCollection.Add(new NpgsqlParameter(paramName, npgsqlDbType) { Value = $"%{validValue_Contains}%" });
                                }
                                else
                                {
                                    conditions.Add("1=0");
                                }
                            }
                            else
                            {
                                conditions.Add("1=0");
                            }
                            break;

                        case FilterOperator.In:
                        case FilterOperator.NotIn:
                            if (filterCondition.Value is System.Collections.IEnumerable enumerable && filterCondition.Value is not string)
                            {
                                List<object> validValues = [];
                                foreach (object item in enumerable)
                                {
                                    if (column.TryGetValidValue(item, out object? validItem) && validItem is not null)
                                    {
                                        validValues.Add(validItem);
                                    }
                                }

                                if (validValues.Count == 0)
                                {
                                    conditions.Add(filterCondition.FilterOperator == FilterOperator.In ? "1=0" : "1=1");
                                }
                                else if (validValues.Count == 1)
                                {
                                    string paramName = $"filterParam_{parameterIndex}";
                                    parameterIndex++;
                                    string opSymbol = filterCondition.FilterOperator == FilterOperator.In ? "=" : "!=";

                                    conditions.Add($"\"{filterCondition.ColumnUniqueId}\" {opSymbol} @{paramName}");
                                    npgsqlParameterCollection.Add(new NpgsqlParameter(paramName, npgsqlDbType) { Value = validValues[0] });
                                }
                                else
                                {
                                    string paramName = $"filterParam_{parameterIndex}";
                                    parameterIndex++;
                                    string arrayFunc = filterCondition.FilterOperator == FilterOperator.In ? "= ANY" : "!= ALL";

                                    conditions.Add($"\"{filterCondition.ColumnUniqueId}\" {arrayFunc}(@{paramName})");
                                    npgsqlParameterCollection.Add(new NpgsqlParameter(paramName, NpgsqlTypes.NpgsqlDbType.Array | npgsqlDbType) { Value = validValues.ToArray() });
                                }
                            }
                            else
                            {
                                if (column.TryGetValidValue(filterCondition.Value, out object? validValue) && validValue is not null)
                                {
                                    string paramName = $"filterParam_{parameterIndex}";
                                    parameterIndex++;
                                    string opSymbol = filterCondition.FilterOperator == FilterOperator.In ? "=" : "!=";

                                    conditions.Add($"\"{filterCondition.ColumnUniqueId}\" {opSymbol} @{paramName}");
                                    npgsqlParameterCollection.Add(new NpgsqlParameter(paramName, npgsqlDbType) { Value = validValue });
                                }
                                else
                                {
                                    conditions.Add(filterCondition.FilterOperator == FilterOperator.In ? "1=0" : "1=1");
                                }
                            }
                            break;

                        default: // Equals, NotEquals, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual
                            if (column.TryGetValidValue(filterCondition.Value, out object? validValue_Comp))
                            {
                                if (validValue_Comp is not null && validValue_Comp != System.DBNull.Value)
                                {
                                    string paramName = $"filterParam_{parameterIndex}";
                                    parameterIndex++;
                                    string opSymbol = filterCondition.FilterOperator switch
                                    {
                                        FilterOperator.Equals => "=",
                                        FilterOperator.NotEquals => "!=",
                                        FilterOperator.GreaterThan => ">",
                                        FilterOperator.GreaterThanOrEqual => ">=",
                                        FilterOperator.LessThan => "<",
                                        FilterOperator.LessThanOrEqual => "<=",
                                        _ => "="
                                    };

                                    conditions.Add($"\"{filterCondition.ColumnUniqueId}\" {opSymbol} @{paramName}");
                                    npgsqlParameterCollection.Add(new NpgsqlParameter(paramName, npgsqlDbType) { Value = validValue_Comp });
                                }
                                else
                                {
                                    if (filterCondition.FilterOperator == FilterOperator.Equals)
                                    {
                                        conditions.Add($"\"{filterCondition.ColumnUniqueId}\" IS NULL");
                                    }
                                    else if (filterCondition.FilterOperator == FilterOperator.NotEquals)
                                    {
                                        conditions.Add($"\"{filterCondition.ColumnUniqueId}\" IS NOT NULL");
                                    }
                                    else
                                    {
                                        conditions.Add("1=0");
                                    }
                                }
                            }
                            else
                            {
                                conditions.Add("1=0");
                            }
                            break;
                    }
                }
            }

            if (filterGroup.FilterGroups is not null)
            {
                foreach (FilterGroup filterGroup_Child in filterGroup.FilterGroups)
                {
                    if (filterGroup_Child is null)
                    {
                        continue;
                    }

                    StringBuilder stringBuilder_Child = new();
                    if (!filterGroup_Child.TryBuildFilterGroupSql(existingColumns, stringBuilder_Child, npgsqlParameterCollection, ref parameterIndex))
                    {
                        return false;
                    }

                    if (stringBuilder_Child.Length > 0)
                    {
                        conditions.Add(stringBuilder_Child.ToString());
                    }
                }
            }

            if (conditions.Count == 0)
            {
                return true;
            }

            string logicalOp = filterGroup.LogicalOperator == FilterLogicalOperator.Or ? " OR " : " AND ";
            if (conditions.Count == 1)
            {
                stringBuilder_Sql.Append(conditions[0]);
            }
            else
            {
                stringBuilder_Sql.Append('(').Append(string.Join(logicalOp, conditions)).Append(')');
            }

            return true;
        }
    }
}