#### [DiGi\.PostgreSQL\.Table](DiGi.PostgreSQL.Table.Overview.md 'DiGi\.PostgreSQL\.Table\.Overview')

## DiGi\.PostgreSQL\.Table\.Enums Namespace
### Enums

<a name='DiGi.PostgreSQL.Table.Enums.FilterLogicalOperator'></a>

## FilterLogicalOperator Enum

Specifies the logical operator to combine multiple filter conditions or groups\.

```csharp
public enum FilterLogicalOperator
```
### Fields

<a name='DiGi.PostgreSQL.Table.Enums.FilterLogicalOperator.And'></a>

`And` 0

Combines conditions or groups using logical AND\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterLogicalOperator.Or'></a>

`Or` 1

Combines conditions or groups using logical OR\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator'></a>

## FilterOperator Enum

Specifies the comparison operator to be applied to a database column filter\.

```csharp
public enum FilterOperator
```
### Fields

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.Equals'></a>

`Equals` 0

Checks if the column value is equal to the filter value\. Applicable to both text and numeric columns\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.NotEquals'></a>

`NotEquals` 1

Checks if the column value is not equal to the filter value\. Applicable to both text and numeric columns\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.GreaterThan'></a>

`GreaterThan` 2

Checks if the column value is greater than the filter value\. Applicable to numeric columns only\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.GreaterThanOrEqual'></a>

`GreaterThanOrEqual` 3

Checks if the column value is greater than or equal to the filter value\. Applicable to numeric columns only\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.LessThan'></a>

`LessThan` 4

Checks if the column value is less than the filter value\. Applicable to numeric columns only\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.LessThanOrEqual'></a>

`LessThanOrEqual` 5

Checks if the column value is less than or equal to the filter value\. Applicable to numeric columns only\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.In'></a>

`In` 6

Checks if the column value matches any of the values in the specified collection parameter\. Applicable to both text and numeric columns\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.NotIn'></a>

`NotIn` 7

Checks if the column value does not match any of the values in the specified collection parameter\. Applicable to both text and numeric columns\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.Contains'></a>

`Contains` 8

Checks if the column value contains the filter string value as a substring \(case\-insensitive search\)\. Applicable to text and string columns only\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.IsNull'></a>

`IsNull` 9

Checks if the column value is null\. Applicable to all column types\.

<a name='DiGi.PostgreSQL.Table.Enums.FilterOperator.IsNotNull'></a>

`IsNotNull` 10

Checks if the column value is not null\. Applicable to all column types\.

<a name='DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction'></a>

## MultivalueAggregateFunction Enum

Specifies statistical and text\-parsing aggregation calculations for multi\-value column operations\.

```csharp
public enum MultivalueAggregateFunction
```
### Fields

<a name='DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction.SplitDistinctCount'></a>

`SplitDistinctCount` 0

Splits multi\-value string items by a separator, and counts unique sub\-items\.

<a name='DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction.SplitValueDistribution'></a>

`SplitValueDistribution` 1

Splits multi\-value string items by a separator, groups them, and counts sub\-item frequencies\.

<a name='DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction'></a>

## SinglevalueAggregateFunction Enum

Specifies statistical aggregation calculations for single\-value column operations\.

```csharp
public enum SinglevalueAggregateFunction
```
### Fields

<a name='DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction.Avg'></a>

`Avg` 0

Calculates the average value of a column\.

<a name='DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction.Sum'></a>

`Sum` 1

Calculates the sum total of a column\.

<a name='DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction.Min'></a>

`Min` 2

Finds the minimum value in a column\.

<a name='DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction.Max'></a>

`Max` 3

Finds the maximum value in a column\.

<a name='DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction.Count'></a>

`Count` 4

Counts the number of non\-null records in a column\.

<a name='DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction.DistinctCount'></a>

`DistinctCount` 5

Counts the unique values in a column\.