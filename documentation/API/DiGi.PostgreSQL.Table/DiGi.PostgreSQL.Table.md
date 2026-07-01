#### [DiGi\.PostgreSQL\.Table](index.md 'index')

## DiGi\.PostgreSQL\.Table Namespace
### Classes

<a name='DiGi.PostgreSQL.Table.Convert'></a>

## Convert Class

```csharp
public static class Convert
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Convert
### Methods

<a name='DiGi.PostgreSQL.Table.Convert.ToDiGi_UColumn_(thisUColumn)'></a>

## Convert\.ToDiGi\<UColumn\>\(this UColumn\) Method

Converts a column implementation to a DiGi core column representation\.

```csharp
public static DiGi.PostgreSQL.Table.Classes.Column? ToDiGi<UColumn>(this UColumn? column)
    where UColumn : DiGi.Core.IO.Table.Interfaces.IColumn;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Convert.ToDiGi_UColumn_(thisUColumn).UColumn'></a>

`UColumn`

The type of the column being converted, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Convert.ToDiGi_UColumn_(thisUColumn).column'></a>

`column` [UColumn](DiGi.PostgreSQL.Table.md#DiGi.PostgreSQL.Table.Convert.ToDiGi_UColumn_(thisUColumn).UColumn 'DiGi\.PostgreSQL\.Table\.Convert\.ToDiGi\<UColumn\>\(this UColumn\)\.UColumn')

The source column instance to convert\.

#### Returns
[Column](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Column 'DiGi\.PostgreSQL\.Table\.Classes\.Column')  
A [Column](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Column 'DiGi\.PostgreSQL\.Table\.Classes\.Column') instance if the input is not null; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Convert.ToDiGi_UTable,UColumn,URow_(thisUTable)'></a>

## Convert\.ToDiGi\<UTable,UColumn,URow\>\(this UTable\) Method

Converts a generic table implementation to a standard [Table](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Table 'DiGi\.PostgreSQL\.Table\.Classes\.Table') object\.

```csharp
public static DiGi.PostgreSQL.Table.Classes.Table? ToDiGi<UTable,UColumn,URow>(this UTable? table)
    where UTable : DiGi.Core.IO.Table.Interfaces.ITable<UColumn, URow>, new()
    where UColumn : DiGi.Core.IO.Table.Interfaces.IColumn
    where URow : DiGi.Core.IO.Table.Interfaces.IRow<URow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Convert.ToDiGi_UTable,UColumn,URow_(thisUTable).UTable'></a>

`UTable`

The type of the table implementing [DiGi\.Core\.IO\.Table\.Interfaces\.ITable&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.itable-2 'DiGi\.Core\.IO\.Table\.Interfaces\.ITable\`2')\.

<a name='DiGi.PostgreSQL.Table.Convert.ToDiGi_UTable,UColumn,URow_(thisUTable).UColumn'></a>

`UColumn`

The type of the column implementing [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn')\.

<a name='DiGi.PostgreSQL.Table.Convert.ToDiGi_UTable,UColumn,URow_(thisUTable).URow'></a>

`URow`

The type of the row implementing [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Convert.ToDiGi_UTable,UColumn,URow_(thisUTable).table'></a>

`table` [UTable](DiGi.PostgreSQL.Table.md#DiGi.PostgreSQL.Table.Convert.ToDiGi_UTable,UColumn,URow_(thisUTable).UTable 'DiGi\.PostgreSQL\.Table\.Convert\.ToDiGi\<UTable,UColumn,URow\>\(this UTable\)\.UTable')

The source table to convert\.

#### Returns
[Table](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Table 'DiGi\.PostgreSQL\.Table\.Classes\.Table')  
A converted [Table](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Table 'DiGi\.PostgreSQL\.Table\.Classes\.Table') instance, or null if the input table is null\.

<a name='DiGi.PostgreSQL.Table.Create'></a>

## Create Class

```csharp
public static class Create
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Create
### Methods

<a name='DiGi.PostgreSQL.Table.Create.TableAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_,System.Collections.Generic.IEnumerable_UColumn_)'></a>

## Create\.TableAsync\<UColumn\>\(this NpgsqlConnection, string, TableConversionOptions\<UColumn\>, IEnumerable\<UColumn\>\) Method

Asynchronously creates a table or adds missing columns to an existing table in the PostgreSQL database based on the provided column definitions and options\.

```csharp
public static System.Threading.Tasks.Task<bool> TableAsync<UColumn>(this Npgsql.NpgsqlConnection? npgsqlConnection, string tableName, DiGi.PostgreSQL.Table.Classes.TableConversionOptions<UColumn>? tableConversionOptions, System.Collections.Generic.IEnumerable<UColumn> columns)
    where UColumn : DiGi.Core.IO.Table.Interfaces.IColumn;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Create.TableAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_,System.Collections.Generic.IEnumerable_UColumn_).UColumn'></a>

`UColumn`

The type of column implementation, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Create.TableAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_,System.Collections.Generic.IEnumerable_UColumn_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to execute the database commands\.

<a name='DiGi.PostgreSQL.Table.Create.TableAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_,System.Collections.Generic.IEnumerable_UColumn_).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the table to be created or modified\.

<a name='DiGi.PostgreSQL.Table.Create.TableAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_,System.Collections.Generic.IEnumerable_UColumn_).tableConversionOptions'></a>

`tableConversionOptions` [DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions&lt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_ 'DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions\<UColumn\>')[UColumn](DiGi.PostgreSQL.Table.md#DiGi.PostgreSQL.Table.Create.TableAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_,System.Collections.Generic.IEnumerable_UColumn_).UColumn 'DiGi\.PostgreSQL\.Table\.Create\.TableAsync\<UColumn\>\(this Npgsql\.NpgsqlConnection, string, DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions\<UColumn\>, System\.Collections\.Generic\.IEnumerable\<UColumn\>\)\.UColumn')[&gt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_ 'DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions\<UColumn\>')

Optional configuration settings for table conversion, such as primary keys and partitioning rules\.

<a name='DiGi.PostgreSQL.Table.Create.TableAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_,System.Collections.Generic.IEnumerable_UColumn_).columns'></a>

`columns` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[UColumn](DiGi.PostgreSQL.Table.md#DiGi.PostgreSQL.Table.Create.TableAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_,System.Collections.Generic.IEnumerable_UColumn_).UColumn 'DiGi\.PostgreSQL\.Table\.Create\.TableAsync\<UColumn\>\(this Npgsql\.NpgsqlConnection, string, DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions\<UColumn\>, System\.Collections\.Generic\.IEnumerable\<UColumn\>\)\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of column definitions to be applied to the table\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the table was successfully created or updated; otherwise, false\.

<a name='DiGi.PostgreSQL.Table.Create.TableAsync_Columns(thisNpgsql.NpgsqlConnection)'></a>

## Create\.TableAsync\_Columns\(this NpgsqlConnection\) Method

Initializes the metadata repository for dynamic column management\.
This table tracks all custom engineering parameters added to the partitioned main tables\.

```csharp
public static System.Threading.Tasks.Task<bool> TableAsync_Columns(this Npgsql.NpgsqlConnection? npgsqlConnection);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Create.TableAsync_Columns(thisNpgsql.NpgsqlConnection).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to create the columns metadata table\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the repository was successfully initialized; otherwise, false\.

<a name='DiGi.PostgreSQL.Table.Modify'></a>

## Modify Class

```csharp
public static class Modify
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Modify
### Methods

<a name='DiGi.PostgreSQL.Table.Modify.CollectColumnUniqueIds(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.HashSet_string_)'></a>

## Modify\.CollectColumnUniqueIds\(this FilterGroup, HashSet\<string\>\) Method

Recursively traverses a [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup') to collect all unique column identifiers\.

```csharp
public static void CollectColumnUniqueIds(this DiGi.PostgreSQL.Table.Classes.FilterGroup? filterGroup, System.Collections.Generic.HashSet<string> uniqueIds);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Modify.CollectColumnUniqueIds(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.HashSet_string_).filterGroup'></a>

`filterGroup` [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')

The filter group instance to traverse\.

<a name='DiGi.PostgreSQL.Table.Modify.CollectColumnUniqueIds(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.HashSet_string_).uniqueIds'></a>

`uniqueIds` [System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')

The set to accumulate unique column identifiers in\.

<a name='DiGi.PostgreSQL.Table.Modify.UpdateAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_UColumn_)'></a>

## Modify\.UpdateAsync\<UColumn\>\(this NpgsqlConnection, string, IEnumerable\<UColumn\>\) Method

Updates or inserts column definitions into the PostgreSQL database for a specified table using an upsert operation\.

```csharp
public static System.Threading.Tasks.Task<bool> UpdateAsync<UColumn>(this Npgsql.NpgsqlConnection? npgsqlConnection, string tableName, System.Collections.Generic.IEnumerable<UColumn> columns)
    where UColumn : DiGi.Core.IO.Table.Interfaces.IColumn;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Modify.UpdateAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_UColumn_).UColumn'></a>

`UColumn`

The type of the column being updated, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Modify.UpdateAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_UColumn_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection') instance used to communicate with the PostgreSQL database\.

<a name='DiGi.PostgreSQL.Table.Modify.UpdateAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_UColumn_).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the table whose columns are being updated\.

<a name='DiGi.PostgreSQL.Table.Modify.UpdateAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_UColumn_).columns'></a>

`columns` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[UColumn](DiGi.PostgreSQL.Table.md#DiGi.PostgreSQL.Table.Modify.UpdateAsync_UColumn_(thisNpgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_UColumn_).UColumn 'DiGi\.PostgreSQL\.Table\.Modify\.UpdateAsync\<UColumn\>\(this Npgsql\.NpgsqlConnection, string, System\.Collections\.Generic\.IEnumerable\<UColumn\>\)\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of column objects implementing [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn') to be updated or inserted\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is `true` if one or more rows were affected; otherwise, `false`\.

<a name='DiGi.PostgreSQL.Table.Query'></a>

## Query Class

Static partial class containing query extension methods for database operations\.

```csharp
public static class Query
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Query
### Methods

<a name='DiGi.PostgreSQL.Table.Query.DataTypeName(thisDiGi.Core.IO.Table.Interfaces.IColumn)'></a>

## Query\.DataTypeName\(this IColumn\) Method

Gets the PostgreSQL data type name for the specified column\.

```csharp
public static string? DataTypeName(this DiGi.Core.IO.Table.Interfaces.IColumn? column);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Query.DataTypeName(thisDiGi.Core.IO.Table.Interfaces.IColumn).column'></a>

`column` [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn')

The column for which to get the data type name\.

#### Returns
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')  
The PostgreSQL data type name as a string, or null if not found\.

<a name='DiGi.PostgreSQL.Table.Query.DataTypeName(thisSystem.Type)'></a>

## Query\.DataTypeName\(this Type\) Method

Gets the PostgreSQL data type name for the specified \.NET type\.

```csharp
public static string? DataTypeName(this System.Type? type);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Query.DataTypeName(thisSystem.Type).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The \.NET type for which to get the data type name\.

#### Returns
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')  
The PostgreSQL data type name as a string, or null if not found\.

<a name='DiGi.PostgreSQL.Table.Query.NpgsqlDbType(thisDiGi.Core.IO.Table.Interfaces.IColumn)'></a>

## Query\.NpgsqlDbType\(this IColumn\) Method

Maps an [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn') to its corresponding [NpgsqlTypes\.NpgsqlDbType](https://learn.microsoft.com/en-us/dotnet/api/npgsqltypes.npgsqldbtype 'NpgsqlTypes\.NpgsqlDbType')\.

```csharp
public static System.Nullable<NpgsqlTypes.NpgsqlDbType> NpgsqlDbType(this DiGi.Core.IO.Table.Interfaces.IColumn? column);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Query.NpgsqlDbType(thisDiGi.Core.IO.Table.Interfaces.IColumn).column'></a>

`column` [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn')

The column for which the PostgreSQL data type is being determined\.

#### Returns
[System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[NpgsqlTypes\.NpgsqlDbType](https://learn.microsoft.com/en-us/dotnet/api/npgsqltypes.npgsqldbtype 'NpgsqlTypes\.NpgsqlDbType')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')  
The mapped [NpgsqlTypes\.NpgsqlDbType](https://learn.microsoft.com/en-us/dotnet/api/npgsqltypes.npgsqldbtype 'NpgsqlTypes\.NpgsqlDbType'), or [null](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/null 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/keywords/null') if the mapping cannot be determined\.

<a name='DiGi.PostgreSQL.Table.Query.NpgsqlDbType(thisSystem.Type)'></a>

## Query\.NpgsqlDbType\(this Type\) Method

Maps a \.NET [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type') to its corresponding [NpgsqlTypes\.NpgsqlDbType](https://learn.microsoft.com/en-us/dotnet/api/npgsqltypes.npgsqldbtype 'NpgsqlTypes\.NpgsqlDbType')\.

```csharp
public static System.Nullable<NpgsqlTypes.NpgsqlDbType> NpgsqlDbType(this System.Type? type);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Query.NpgsqlDbType(thisSystem.Type).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The \.NET type for which the PostgreSQL data type is being determined\.

#### Returns
[System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[NpgsqlTypes\.NpgsqlDbType](https://learn.microsoft.com/en-us/dotnet/api/npgsqltypes.npgsqldbtype 'NpgsqlTypes\.NpgsqlDbType')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')  
The mapped [NpgsqlTypes\.NpgsqlDbType](https://learn.microsoft.com/en-us/dotnet/api/npgsqltypes.npgsqldbtype 'NpgsqlTypes\.NpgsqlDbType'), or [null](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/null 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/keywords/null') if the mapping cannot be determined\.

<a name='DiGi.PostgreSQL.Table.Query.PartitionNameSuffix(object)'></a>

## Query\.PartitionNameSuffix\(object\) Method

Converts the provided value into a formatted partition name suffix by converting it to lowercase, trimming whitespace, and replacing spaces with underscores\.

```csharp
public static string? PartitionNameSuffix(object? value);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Query.PartitionNameSuffix(object).value'></a>

`value` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The object value to be processed\.

#### Returns
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')  
A formatted string representing the partition name suffix, or null if the input is not a valid string\.

<a name='DiGi.PostgreSQL.Table.Query.TryBuildFilterGroupSql_UColumn_(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.List_UColumn_,System.Text.StringBuilder,Npgsql.NpgsqlParameterCollection,int)'></a>

## Query\.TryBuildFilterGroupSql\<UColumn\>\(this FilterGroup, List\<UColumn\>, StringBuilder, NpgsqlParameterCollection, int\) Method

Recursively builds the SQL query condition and parameters from the specified [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')\.

```csharp
public static bool TryBuildFilterGroupSql<UColumn>(this DiGi.PostgreSQL.Table.Classes.FilterGroup? filterGroup, System.Collections.Generic.List<UColumn> existingColumns, System.Text.StringBuilder stringBuilder_Sql, Npgsql.NpgsqlParameterCollection npgsqlParameterCollection, ref int parameterIndex)
    where UColumn : DiGi.Core.IO.Table.Interfaces.IColumn;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Query.TryBuildFilterGroupSql_UColumn_(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.List_UColumn_,System.Text.StringBuilder,Npgsql.NpgsqlParameterCollection,int).UColumn'></a>

`UColumn`

The base column type deriving from [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Query.TryBuildFilterGroupSql_UColumn_(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.List_UColumn_,System.Text.StringBuilder,Npgsql.NpgsqlParameterCollection,int).filterGroup'></a>

`filterGroup` [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')

The filter group instance to build SQL for\.

<a name='DiGi.PostgreSQL.Table.Query.TryBuildFilterGroupSql_UColumn_(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.List_UColumn_,System.Text.StringBuilder,Npgsql.NpgsqlParameterCollection,int).existingColumns'></a>

`existingColumns` [System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UColumn](DiGi.PostgreSQL.Table.md#DiGi.PostgreSQL.Table.Query.TryBuildFilterGroupSql_UColumn_(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.List_UColumn_,System.Text.StringBuilder,Npgsql.NpgsqlParameterCollection,int).UColumn 'DiGi\.PostgreSQL\.Table\.Query\.TryBuildFilterGroupSql\<UColumn\>\(this DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup, System\.Collections\.Generic\.List\<UColumn\>, System\.Text\.StringBuilder, Npgsql\.NpgsqlParameterCollection, int\)\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')

The list of valid whitelisted database columns\.

<a name='DiGi.PostgreSQL.Table.Query.TryBuildFilterGroupSql_UColumn_(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.List_UColumn_,System.Text.StringBuilder,Npgsql.NpgsqlParameterCollection,int).stringBuilder_Sql'></a>

`stringBuilder_Sql` [System\.Text\.StringBuilder](https://learn.microsoft.com/en-us/dotnet/api/system.text.stringbuilder 'System\.Text\.StringBuilder')

The string builder to append the resulting SQL condition to\.

<a name='DiGi.PostgreSQL.Table.Query.TryBuildFilterGroupSql_UColumn_(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.List_UColumn_,System.Text.StringBuilder,Npgsql.NpgsqlParameterCollection,int).npgsqlParameterCollection'></a>

`npgsqlParameterCollection` [Npgsql\.NpgsqlParameterCollection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlparametercollection 'Npgsql\.NpgsqlParameterCollection')

The parameter collection to bind Npgsql parameters to\.

<a name='DiGi.PostgreSQL.Table.Query.TryBuildFilterGroupSql_UColumn_(thisDiGi.PostgreSQL.Table.Classes.FilterGroup,System.Collections.Generic.List_UColumn_,System.Text.StringBuilder,Npgsql.NpgsqlParameterCollection,int).parameterIndex'></a>

`parameterIndex` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

A reference counter for unique query parameter names\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the SQL condition was successfully built; otherwise, false\.

<a name='DiGi.PostgreSQL.Table.Query.TryConvert(thisobject,object,NpgsqlTypes.NpgsqlDbType)'></a>

## Query\.TryConvert\(this object, object, NpgsqlDbType\) Method

Attempts to convert a given object to a value corresponding to the specified NpgsqlDbType\.

```csharp
public static bool TryConvert(this object? @object, out object? result, NpgsqlTypes.NpgsqlDbType npgsqlDbType);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Query.TryConvert(thisobject,object,NpgsqlTypes.NpgsqlDbType).object'></a>

`object` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The source object to be converted\.

<a name='DiGi.PostgreSQL.Table.Query.TryConvert(thisobject,object,NpgsqlTypes.NpgsqlDbType).result'></a>

`result` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

When this method returns, contains the converted value if successful; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Query.TryConvert(thisobject,object,NpgsqlTypes.NpgsqlDbType).npgsqlDbType'></a>

`npgsqlDbType` [NpgsqlTypes\.NpgsqlDbType](https://learn.microsoft.com/en-us/dotnet/api/npgsqltypes.npgsqldbtype 'NpgsqlTypes\.NpgsqlDbType')

The target PostgreSQL database type for conversion\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the conversion was successful; otherwise, false\.

<a name='DiGi.PostgreSQL.Table.Query.UniqueId(thisDiGi.Core.IO.Table.Interfaces.IColumn)'></a>

## Query\.UniqueId\(this IColumn\) Method

Generates a unique identifier for the specified column by normalizing its name\.

```csharp
public static string? UniqueId(this DiGi.Core.IO.Table.Interfaces.IColumn? column);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Query.UniqueId(thisDiGi.Core.IO.Table.Interfaces.IColumn).column'></a>

`column` [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn')

The column instance to process\.

#### Returns
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')  
A normalized string representing the unique identifier, or null if the column or its name is null\.