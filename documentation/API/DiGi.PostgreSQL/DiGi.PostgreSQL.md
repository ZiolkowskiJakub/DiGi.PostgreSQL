#### [DiGi\.PostgreSQL](DiGi.PostgreSQL.Overview.md 'DiGi\.PostgreSQL\.Overview')

## DiGi\.PostgreSQL Namespace
### Classes

<a name='DiGi.PostgreSQL.Convert'></a>

## Convert Class

```csharp
public static class Convert
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Convert
### Methods

<a name='DiGi.PostgreSQL.Convert.ToPostgreSQL(thisDiGi.Core.Interfaces.ISerializableObject,DiGi.PostgreSQL.Enums.DataType,NpgsqlTypes.NpgsqlDbType)'></a>

## Convert\.ToPostgreSQL\(this ISerializableObject, DataType, NpgsqlDbType\) Method

Converts a serializable object to a PostgreSQL\-compatible format based on the specified data type\.

```csharp
public static object? ToPostgreSQL(this DiGi.Core.Interfaces.ISerializableObject serializableObject, DiGi.PostgreSQL.Enums.DataType dataType, out NpgsqlTypes.NpgsqlDbType npgsqlDbType);
```
#### Parameters

<a name='DiGi.PostgreSQL.Convert.ToPostgreSQL(thisDiGi.Core.Interfaces.ISerializableObject,DiGi.PostgreSQL.Enums.DataType,NpgsqlTypes.NpgsqlDbType).serializableObject'></a>

`serializableObject` [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')

The object that implements [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject') to be converted\.

<a name='DiGi.PostgreSQL.Convert.ToPostgreSQL(thisDiGi.Core.Interfaces.ISerializableObject,DiGi.PostgreSQL.Enums.DataType,NpgsqlTypes.NpgsqlDbType).dataType'></a>

`dataType` [DataType](DiGi.PostgreSQL.Enums.md#DiGi.PostgreSQL.Enums.DataType 'DiGi\.PostgreSQL\.Enums\.DataType')

The target [DataType](DiGi.PostgreSQL.Enums.md#DiGi.PostgreSQL.Enums.DataType 'DiGi\.PostgreSQL\.Enums\.DataType') for the conversion\.

<a name='DiGi.PostgreSQL.Convert.ToPostgreSQL(thisDiGi.Core.Interfaces.ISerializableObject,DiGi.PostgreSQL.Enums.DataType,NpgsqlTypes.NpgsqlDbType).npgsqlDbType'></a>

`npgsqlDbType` [NpgsqlTypes\.NpgsqlDbType](https://learn.microsoft.com/en-us/dotnet/api/npgsqltypes.npgsqldbtype 'NpgsqlTypes\.NpgsqlDbType')

When this method returns, contains the corresponding [NpgsqlTypes\.NpgsqlDbType](https://learn.microsoft.com/en-us/dotnet/api/npgsqltypes.npgsqldbtype 'NpgsqlTypes\.NpgsqlDbType') for the PostgreSQL database\.

#### Returns
[System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')  
An object representing the converted value in a format compatible with PostgreSQL, or null if no conversion is defined for the given data type\.

<a name='DiGi.PostgreSQL.Create'></a>

## Create Class

```csharp
public static class Create
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Create
### Methods

<a name='DiGi.PostgreSQL.Create.ConnectionData(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile)'></a>

## Create\.ConnectionData\(PostgreSQLConfigurationFile\) Method

Creates a ConnectionData object from the provided PostgreSQL configuration file\.

```csharp
public static DiGi.PostgreSQL.Classes.ConnectionData? ConnectionData(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile? postgreSQLConfigurationFile);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.ConnectionData(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile).postgreSQLConfigurationFile'></a>

`postgreSQLConfigurationFile` [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile')

The PostgreSQL configuration file containing connection details\.

#### Returns
[ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')  
A ConnectionData instance if the configuration is valid; otherwise, null\.

<a name='DiGi.PostgreSQL.Create.DatabaseAsync(DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## Create\.DatabaseAsync\(ConnectionData\) Method

Asynchronously creates a PostgreSQL database based on the provided connection data\.

```csharp
public static System.Threading.Tasks.Task<bool> DatabaseAsync(DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.DatabaseAsync(DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data containing the database name and server details\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains true if the database was created or already exists; otherwise, false\.

<a name='DiGi.PostgreSQL.Create.DatabaseAsync(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile)'></a>

## Create\.DatabaseAsync\(PostgreSQLConfigurationFile\) Method

Asynchronously creates a PostgreSQL database using settings from a configuration file\.

```csharp
public static System.Threading.Tasks.Task<bool> DatabaseAsync(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile? postgreSQLConfigurationFile);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.DatabaseAsync(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile).postgreSQLConfigurationFile'></a>

`postgreSQLConfigurationFile` [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile')

The configuration file containing the necessary connection and tablespace details\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains true if the database was created or already exists; otherwise, false\.

<a name='DiGi.PostgreSQL.Create.DatabaseAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,string)'></a>

## Create\.DatabaseAsync\(this ConnectionData, string, string\) Method

Asynchronously creates a PostgreSQL database with optional tablespace and directory specifications\.

```csharp
public static System.Threading.Tasks.Task<bool> DatabaseAsync(this DiGi.PostgreSQL.Classes.ConnectionData? connectionData, string? tablespaceName=null, string? directory=null);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.DatabaseAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,string).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data containing the database name and server details\.

<a name='DiGi.PostgreSQL.Create.DatabaseAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,string).tablespaceName'></a>

`tablespaceName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The optional name of the tablespace to be used for the database\.

<a name='DiGi.PostgreSQL.Create.DatabaseAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,string).directory'></a>

`directory` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The optional physical directory path on the server where the tablespace should be located\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains true if the database was created or already exists; otherwise, false\.

<a name='DiGi.PostgreSQL.Create.NpgsqlConnection(DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## Create\.NpgsqlConnection\(ConnectionData\) Method

Creates a new NpgsqlConnection using the provided connection data\.

```csharp
public static Npgsql.NpgsqlConnection? NpgsqlConnection(DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.NpgsqlConnection(DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data containing host, username, and password\.

#### Returns
[Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')  
An instance of NpgsqlConnection if the connection data is valid; otherwise, null\.

<a name='DiGi.PostgreSQL.Create.NpgsqlConnection(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile)'></a>

## Create\.NpgsqlConnection\(PostgreSQLConfigurationFile\) Method

Creates a new NpgsqlConnection using the provided PostgreSQL configuration file\.

```csharp
public static Npgsql.NpgsqlConnection? NpgsqlConnection(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile? postgreSQLConfigurationFile);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.NpgsqlConnection(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile).postgreSQLConfigurationFile'></a>

`postgreSQLConfigurationFile` [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile')

The configuration file containing connection settings\.

#### Returns
[Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')  
An instance of NpgsqlConnection if the configuration is valid; otherwise, null\.

<a name='DiGi.PostgreSQL.Create.PostgreSQLConfigurationFile(string)'></a>

## Create\.PostgreSQLConfigurationFile\(string\) Method

Creates a new instance of a PostgreSQL configuration file from the specified path\.

```csharp
public static DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile? PostgreSQLConfigurationFile(string? path);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.PostgreSQLConfigurationFile(string).path'></a>

`path` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The path to the configuration file\.

#### Returns
[PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile')  
A [PostgreSQLConfigurationFile\(string\)](DiGi.PostgreSQL.md#DiGi.PostgreSQL.Create.PostgreSQLConfigurationFile(string) 'DiGi\.PostgreSQL\.Create\.PostgreSQLConfigurationFile\(string\)') if successful; otherwise, null\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Objects(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Enums.DataType,bool,bool)'></a>

## Create\.TableAsync\_Objects\(this NpgsqlConnection, DataType, bool, bool\) Method

Asynchronously creates the main objects table for a specific data type, including optional GIN indexing and type column referencing\.

```csharp
public static System.Threading.Tasks.Task<bool> TableAsync_Objects(this Npgsql.NpgsqlConnection? npgsqlConnection, DiGi.PostgreSQL.Enums.DataType dataType, bool useGIN=false, bool includeType=false);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.TableAsync_Objects(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Enums.DataType,bool,bool).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance used to execute the command\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Objects(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Enums.DataType,bool,bool).dataType'></a>

`dataType` [DataType](DiGi.PostgreSQL.Enums.md#DiGi.PostgreSQL.Enums.DataType 'DiGi\.PostgreSQL\.Enums\.DataType')

The data type that determines the table name and storage format\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Objects(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Enums.DataType,bool,bool).useGIN'></a>

`useGIN` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether a GIN index should be created for JSON data types\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Objects(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Enums.DataType,bool,bool).includeType'></a>

`includeType` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to include a reference column to the types lookup table\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the table was created successfully; otherwise, false\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Objects_Partition(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Enums.DataType,short)'></a>

## Create\.TableAsync\_Objects\_Partition\(this NpgsqlConnection, DataType, short\) Method

Asynchronously creates a specific partition for the objects table based on the provided data type and partition identifier\.

```csharp
public static System.Threading.Tasks.Task<bool> TableAsync_Objects_Partition(this Npgsql.NpgsqlConnection? npgsqlConnection, DiGi.PostgreSQL.Enums.DataType dataType, short partitionId);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.TableAsync_Objects_Partition(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Enums.DataType,short).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance used to execute the command\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Objects_Partition(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Enums.DataType,short).dataType'></a>

`dataType` [DataType](DiGi.PostgreSQL.Enums.md#DiGi.PostgreSQL.Enums.DataType 'DiGi\.PostgreSQL\.Enums\.DataType')

The data type associated with the parent objects table\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Objects_Partition(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Enums.DataType,short).partitionId'></a>

`partitionId` [System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')

The unique identifier for the partition being created\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the partition was created successfully; otherwise, false\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Partition_T_(thisNpgsql.NpgsqlConnection,string,string,System.Collections.Generic.IEnumerable_T_,System.Func_T,string_)'></a>

## Create\.TableAsync\_Partition\<T\>\(this NpgsqlConnection, string, string, IEnumerable\<T\>, Func\<T,string\>\) Method

Asynchronously creates a partition for a specified parent table based on a collection of provided values\.

```csharp
public static System.Threading.Tasks.Task<bool> TableAsync_Partition<T>(this Npgsql.NpgsqlConnection? npgsqlConnection, string tableName, string partitionNameSufix, System.Collections.Generic.IEnumerable<T> values, System.Func<T,string>? conversionFunc=null);
```
#### Type parameters

<a name='DiGi.PostgreSQL.Create.TableAsync_Partition_T_(thisNpgsql.NpgsqlConnection,string,string,System.Collections.Generic.IEnumerable_T_,System.Func_T,string_).T'></a>

`T`

The type of the elements in the values collection\.
#### Parameters

<a name='DiGi.PostgreSQL.Create.TableAsync_Partition_T_(thisNpgsql.NpgsqlConnection,string,string,System.Collections.Generic.IEnumerable_T_,System.Func_T,string_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance used to execute the command\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Partition_T_(thisNpgsql.NpgsqlConnection,string,string,System.Collections.Generic.IEnumerable_T_,System.Func_T,string_).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the parent table that is being partitioned\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Partition_T_(thisNpgsql.NpgsqlConnection,string,string,System.Collections.Generic.IEnumerable_T_,System.Func_T,string_).partitionNameSufix'></a>

`partitionNameSufix` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The suffix to be appended to the parent table name to create the partition table name\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Partition_T_(thisNpgsql.NpgsqlConnection,string,string,System.Collections.Generic.IEnumerable_T_,System.Func_T,string_).values'></a>

`values` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[T](DiGi.PostgreSQL.md#DiGi.PostgreSQL.Create.TableAsync_Partition_T_(thisNpgsql.NpgsqlConnection,string,string,System.Collections.Generic.IEnumerable_T_,System.Func_T,string_).T 'DiGi\.PostgreSQL\.Create\.TableAsync\_Partition\<T\>\(this Npgsql\.NpgsqlConnection, string, string, System\.Collections\.Generic\.IEnumerable\<T\>, System\.Func\<T,string\>\)\.T')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of values for which this partition will be responsible\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Partition_T_(thisNpgsql.NpgsqlConnection,string,string,System.Collections.Generic.IEnumerable_T_,System.Func_T,string_).conversionFunc'></a>

`conversionFunc` [System\.Func&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')[T](DiGi.PostgreSQL.md#DiGi.PostgreSQL.Create.TableAsync_Partition_T_(thisNpgsql.NpgsqlConnection,string,string,System.Collections.Generic.IEnumerable_T_,System.Func_T,string_).T 'DiGi\.PostgreSQL\.Create\.TableAsync\_Partition\<T\>\(this Npgsql\.NpgsqlConnection, string, string, System\.Collections\.Generic\.IEnumerable\<T\>, System\.Func\<T,string\>\)\.T')[,](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')

An optional function to convert each value of type T into a string representation for the SQL command\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the partition was created successfully; otherwise, false\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Partitions(thisNpgsql.NpgsqlConnection)'></a>

## Create\.TableAsync\_Partitions\(this NpgsqlConnection\) Method

Asynchronously creates the partitions lookup table used to manage and track data partitioning\.

```csharp
public static System.Threading.Tasks.Task<bool> TableAsync_Partitions(this Npgsql.NpgsqlConnection? npgsqlConnection);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.TableAsync_Partitions(thisNpgsql.NpgsqlConnection).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance used to execute the command\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the partitions table was created successfully; otherwise, false\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Partition_Default(thisNpgsql.NpgsqlConnection,string)'></a>

## Create\.TableAsync\_Partition\_Default\(this NpgsqlConnection, string\) Method

Asynchronously creates a default partition for the specified parent table to handle any values not matched by other partitions\.

```csharp
public static System.Threading.Tasks.Task<bool> TableAsync_Partition_Default(this Npgsql.NpgsqlConnection? npgsqlConnection, string tableName);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.TableAsync_Partition_Default(thisNpgsql.NpgsqlConnection,string).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance used to execute the command\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Partition_Default(thisNpgsql.NpgsqlConnection,string).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the parent table for which the default partition is created\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the default partition was created successfully; otherwise, false\.

<a name='DiGi.PostgreSQL.Create.TableAsync_Types(thisNpgsql.NpgsqlConnection)'></a>

## Create\.TableAsync\_Types\(this NpgsqlConnection\) Method

Asynchronously creates the 'types' lookup table in the PostgreSQL database to optimize storage and filtering, including a timestamp for auditing when the type was first introduced\.

```csharp
public static System.Threading.Tasks.Task<bool> TableAsync_Types(this Npgsql.NpgsqlConnection? npgsqlConnection);
```
#### Parameters

<a name='DiGi.PostgreSQL.Create.TableAsync_Types(thisNpgsql.NpgsqlConnection).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection') instance used to execute the create table command\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the table was created successfully or already exists; otherwise, false\.

<a name='DiGi.PostgreSQL.Modify'></a>

## Modify Class

```csharp
public static class Modify
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Modify
### Methods

<a name='DiGi.PostgreSQL.Modify.Analyze(Npgsql.NpgsqlConnection,string,int)'></a>

## Modify\.Analyze\(NpgsqlConnection, string, int\) Method

Performs an ANALYZE operation on the specified table to update statistics for the query planner\.

```csharp
public static System.Threading.Tasks.Task<bool> Analyze(Npgsql.NpgsqlConnection? npgsqlConnection, string? tableName, int commandTimeout=30);
```
#### Parameters

<a name='DiGi.PostgreSQL.Modify.Analyze(Npgsql.NpgsqlConnection,string,int).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection used to execute the command\.

<a name='DiGi.PostgreSQL.Modify.Analyze(Npgsql.NpgsqlConnection,string,int).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the table to be analyzed\.

<a name='DiGi.PostgreSQL.Modify.Analyze(Npgsql.NpgsqlConnection,string,int).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the analysis was successful; otherwise, false\.

<a name='DiGi.PostgreSQL.Modify.CleanPartitionsAsync(Npgsql.NpgsqlConnection)'></a>

## Modify\.CleanPartitionsAsync\(NpgsqlConnection\) Method

Asynchronously cleans up partitions by removing empty ones from the metadata and dropping physical tables if they contain no rows\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Classes.Partition>?> CleanPartitionsAsync(Npgsql.NpgsqlConnection? npgsqlConnection);
```
#### Parameters

<a name='DiGi.PostgreSQL.Modify.CleanPartitionsAsync(Npgsql.NpgsqlConnection).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection to be used for the cleanup process\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A list of partitions that were removed, or [null](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/null 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/keywords/null') if the provided connection is null or partition data could not be retrieved\.

<a name='DiGi.PostgreSQL.Modify.ClearAsync(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken)'></a>

## Modify\.ClearAsync\(NpgsqlConnection, string, int, CancellationToken\) Method

Asynchronously clears all data from the specified table and restarts its identity sequence\.

```csharp
public static System.Threading.Tasks.Task<bool> ClearAsync(Npgsql.NpgsqlConnection? npgsqlConnection, string tableName, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Modify.ClearAsync(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to be used for the operation\.

<a name='DiGi.PostgreSQL.Modify.ClearAsync(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the database table to clear\.

<a name='DiGi.PostgreSQL.Modify.ClearAsync(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Modify.ClearAsync(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The cancellation token to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the operation succeeded; otherwise, false\.

<a name='DiGi.PostgreSQL.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_)'></a>

## Modify\.RemoveAsync\(NpgsqlConnection, IEnumerable\<short\>\) Method

Asynchronously removes records from the database partitions associated with the specified partition identifiers\.

```csharp
public static System.Threading.Tasks.Task<bool> RemoveAsync(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<short>? partitionIds);
```
#### Parameters

<a name='DiGi.PostgreSQL.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to be used for the database operation\.

<a name='DiGi.PostgreSQL.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_).partitionIds'></a>

`partitionIds` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of partition identifiers to remove\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if records were removed; otherwise, false\.

<a name='DiGi.PostgreSQL.Modify.RemoveDatabaseAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,string)'></a>

## Modify\.RemoveDatabaseAsync\(this ConnectionData, string, string\) Method

Asynchronously removes a specified database and its associated tablespace from the PostgreSQL server\.

```csharp
public static System.Threading.Tasks.Task<bool> RemoveDatabaseAsync(this DiGi.PostgreSQL.Classes.ConnectionData? connectionData, string databaseName, string tablespaceName);
```
#### Parameters

<a name='DiGi.PostgreSQL.Modify.RemoveDatabaseAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,string).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to connect to the PostgreSQL server\.

<a name='DiGi.PostgreSQL.Modify.RemoveDatabaseAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,string).databaseName'></a>

`databaseName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the database to be removed\.

<a name='DiGi.PostgreSQL.Modify.RemoveDatabaseAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,string).tablespaceName'></a>

`tablespaceName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the tablespace associated with the database to be removed\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if the removal was successful; otherwise, false\.

<a name='DiGi.PostgreSQL.Modify.RemoveTableAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string)'></a>

## Modify\.RemoveTableAsync\(this ConnectionData, string\) Method

Asynchronously removes a table from the database if it exists\.

```csharp
public static System.Threading.Tasks.Task<bool> RemoveTableAsync(this DiGi.PostgreSQL.Classes.ConnectionData? connectionData, string tableName);
```
#### Parameters

<a name='DiGi.PostgreSQL.Modify.RemoveTableAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to establish the database connection\.

<a name='DiGi.PostgreSQL.Modify.RemoveTableAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the table to be removed\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains true if the table was successfully removed or did not exist; otherwise, false\.

<a name='DiGi.PostgreSQL.Modify.RemoveTablespaceAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string)'></a>

## Modify\.RemoveTablespaceAsync\(this ConnectionData, string\) Method

Asynchronously removes the specified tablespace from the PostgreSQL server if it is not currently in use by any database\.

```csharp
public static System.Threading.Tasks.Task<bool> RemoveTablespaceAsync(this DiGi.PostgreSQL.Classes.ConnectionData? connectionData, string tablespaceName);
```
#### Parameters

<a name='DiGi.PostgreSQL.Modify.RemoveTablespaceAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to establish a connection to the PostgreSQL server\.

<a name='DiGi.PostgreSQL.Modify.RemoveTablespaceAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string).tablespaceName'></a>

`tablespaceName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the tablespace to be removed\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is `true` if the tablespace was successfully removed or did not exist; otherwise, `false`\.

<a name='DiGi.PostgreSQL.Modify.UpdatePartitionIdAsync(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Enums.DataType)'></a>

## Modify\.UpdatePartitionIdAsync\(this NpgsqlConnection, string, DataType\) Method

Updates or creates a partition ID based on the provided name and data type\.

```csharp
public static System.Threading.Tasks.Task<DiGi.PostgreSQL.Classes.Partition?> UpdatePartitionIdAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, string? name, DiGi.PostgreSQL.Enums.DataType dataType);
```
#### Parameters

<a name='DiGi.PostgreSQL.Modify.UpdatePartitionIdAsync(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Enums.DataType).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for database operations\.

<a name='DiGi.PostgreSQL.Modify.UpdatePartitionIdAsync(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Enums.DataType).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition\.

<a name='DiGi.PostgreSQL.Modify.UpdatePartitionIdAsync(thisNpgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Enums.DataType).dataType'></a>

`dataType` [DataType](DiGi.PostgreSQL.Enums.md#DiGi.PostgreSQL.Enums.DataType 'DiGi\.PostgreSQL\.Enums\.DataType')

The data type associated with the partition\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the updated or created [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition') object, or [null](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/null 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/keywords/null') if the operation failed\.

<a name='DiGi.PostgreSQL.Query'></a>

## Query Class

```csharp
public static class Query
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Query
### Methods

<a name='DiGi.PostgreSQL.Query.ColumnNamesAsync(thisNpgsql.NpgsqlConnection,string,System.Threading.CancellationToken)'></a>

## Query\.ColumnNamesAsync\(this NpgsqlConnection, string, CancellationToken\) Method

Asynchronously retrieves the column names for a specified table from the PostgreSQL database\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<string>?> ColumnNamesAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, string? tableName, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.ColumnNamesAsync(thisNpgsql.NpgsqlConnection,string,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection') used to connect to the database\.

<a name='DiGi.PostgreSQL.Query.ColumnNamesAsync(thisNpgsql.NpgsqlConnection,string,System.Threading.CancellationToken).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the table to retrieve columns for\.

<a name='DiGi.PostgreSQL.Query.ColumnNamesAsync(thisNpgsql.NpgsqlConnection,string,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of column names in lowercase, or null if the connection is null or the table name is null or whitespace\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, Partition\) Method

Asynchronously checks whether any records exist within the provided partition\.

```csharp
public static System.Threading.Tasks.Task<bool> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, DiGi.PostgreSQL.Classes.Partition partition);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition).partition'></a>

`partition` [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')

The partition object to check\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if records exist, otherwise false\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition,System.Collections.Generic.IEnumerable_string_)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, Partition, IEnumerable\<string\>\) Method

Asynchronously checks which of the specified unique identifiers exist within the provided partition\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<string>?> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, DiGi.PostgreSQL.Classes.Partition? partition, System.Collections.Generic.IEnumerable<string>? uniqueIds);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition,System.Collections.Generic.IEnumerable_string_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition,System.Collections.Generic.IEnumerable_string_).partition'></a>

`partition` [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')

The partition object to check\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition,System.Collections.Generic.IEnumerable_string_).uniqueIds'></a>

`uniqueIds` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of unique identifiers to verify\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a set of existing unique identifiers, or null if any input is null\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,string)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, string\) Method

Asynchronously checks whether any records exist within a partition identified by its name\.

```csharp
public static System.Threading.Tasks.Task<bool> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, string? name);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,string).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition to check\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if records exist, otherwise false\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Nullable_short_)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, Nullable\<short\>\) Method

Asynchronously checks whether any records exist within a partition identified by its ID\.

```csharp
public static System.Threading.Tasks.Task<bool> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, System.Nullable<short> partitionId);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Nullable_short_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Nullable_short_).partitionId'></a>

`partitionId` [System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')

The identifier of the partition to check\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is true if records exist, otherwise false\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Nullable_short_,System.Collections.Generic.IEnumerable_string_)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, Nullable\<short\>, IEnumerable\<string\>\) Method

Asynchronously checks which of the specified unique identifiers exist within a partition identified by its ID\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<string>?> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, System.Nullable<short> partitionId, System.Collections.Generic.IEnumerable<string>? uniqueIds);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Nullable_short_,System.Collections.Generic.IEnumerable_string_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Nullable_short_,System.Collections.Generic.IEnumerable_string_).partitionId'></a>

`partitionId` [System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')

The identifier of the partition to check\.

<a name='DiGi.PostgreSQL.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Nullable_short_,System.Collections.Generic.IEnumerable_string_).uniqueIds'></a>

`uniqueIds` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of unique identifiers to verify\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a set of existing unique identifiers, or null if any input is null\.

<a name='DiGi.PostgreSQL.Query.CountAsync(thisNpgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken)'></a>

## Query\.CountAsync\(this NpgsqlConnection, string, int, CancellationToken\) Method

Asynchronously counts the number of rows in a specified table\.

```csharp
public static System.Threading.Tasks.Task<long> CountAsync(this Npgsql.NpgsqlConnection npgsqlConnection, string tableName, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.CountAsync(thisNpgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.Query.CountAsync(thisNpgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the table to count rows from\.

<a name='DiGi.PostgreSQL.Query.CountAsync(thisNpgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds applied to the count command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Query.CountAsync(thisNpgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The cancellation token to observe\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Int64](https://learn.microsoft.com/en-us/dotnet/api/system.int64 'System\.Int64')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing the total row count or \-1 if an error occurs or the table does not exist\.

<a name='DiGi.PostgreSQL.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_,int,System.Threading.CancellationToken)'></a>

## Query\.CountAsync\(this NpgsqlConnection, IEnumerable\<short\>, int, CancellationToken\) Method

Asynchronously counts the number of rows across multiple partitions based on provided partition IDs\.

```csharp
public static System.Threading.Tasks.Task<long> CountAsync(this Npgsql.NpgsqlConnection npgsqlConnection, System.Collections.Generic.IEnumerable<short> partitionIds, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_,int,System.Threading.CancellationToken).partitionIds'></a>

`partitionIds` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of short integers representing the partition identifiers\.

<a name='DiGi.PostgreSQL.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds applied to every count command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The cancellation token to observe\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Int64](https://learn.microsoft.com/en-us/dotnet/api/system.int64 'System\.Int64')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing the total row count across all matching partitions or \-1 if an error occurs\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountAsync(thisNpgsql.NpgsqlConnection,string,bool,int,System.Threading.CancellationToken)'></a>

## Query\.EstimatedCountAsync\(this NpgsqlConnection, string, bool, int, CancellationToken\) Method

Gets an estimated row count for the specified table in a PostgreSQL database\.

```csharp
public static System.Threading.Tasks.Task<System.Nullable<long>> EstimatedCountAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, string tableName, bool analyze=false, int commandTimeout=600, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.EstimatedCountAsync(thisNpgsql.NpgsqlConnection,string,bool,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountAsync(thisNpgsql.NpgsqlConnection,string,bool,int,System.Threading.CancellationToken).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the table to get the estimate for\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountAsync(thisNpgsql.NpgsqlConnection,string,bool,int,System.Threading.CancellationToken).analyze'></a>

`analyze` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A boolean indicating whether to run VACUUM ANALYZE before fetching the count\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountAsync(thisNpgsql.NpgsqlConnection,string,bool,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds applied to every command executed\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountAsync(thisNpgsql.NpgsqlConnection,string,bool,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[System\.Int64](https://learn.microsoft.com/en-us/dotnet/api/system.int64 'System\.Int64')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
The estimated number of rows as a nullable long, \-1 if the table exists but has not been analysed, or null if the table does not exist or connection is invalid\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,bool,int,int,System.Threading.CancellationToken)'></a>

## Query\.EstimatedCountsAsync\(this NpgsqlConnection, IEnumerable\<string\>, bool, int, int, CancellationToken\) Method

Gets estimated row counts for many tables at once, reading the planner's statistics for the whole set in a single catalog query per batch\.

This is the plural form of [EstimatedCountAsync\(this NpgsqlConnection, string, bool, int, CancellationToken\)](DiGi.PostgreSQL.md#DiGi.PostgreSQL.Query.EstimatedCountAsync(thisNpgsql.NpgsqlConnection,string,bool,int,System.Threading.CancellationToken) 'DiGi\.PostgreSQL\.Query\.EstimatedCountAsync\(this Npgsql\.NpgsqlConnection, string, bool, int, System\.Threading\.CancellationToken\)') and exists because calling the singular in a loop issues two round trips per table - one existence check and one catalog read. Reading `pg_class` by name answers both questions at once: a table that does not exist simply produces no row.

A table is absent from the result when it does not exist, and carries `-1` when it exists but has never been analysed, mirroring the `null` and `-1` the singular returns for those two cases.

Setting [analyze](DiGi.PostgreSQL.md#DiGi.PostgreSQL.Query.EstimatedCountsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,bool,int,int,System.Threading.CancellationToken).analyze 'DiGi\.PostgreSQL\.Query\.EstimatedCountsAsync\(this Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<string\>, bool, int, int, System\.Threading\.CancellationToken\)\.analyze') costs one `VACUUM ANALYZE` statement per existing table. That work is per table by construction and cannot be batched, so the cost grows with the size of [tableNames](DiGi.PostgreSQL.md#DiGi.PostgreSQL.Query.EstimatedCountsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,bool,int,int,System.Threading.CancellationToken).tableNames 'DiGi\.PostgreSQL\.Query\.EstimatedCountsAsync\(this Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<string\>, bool, int, int, System\.Threading\.CancellationToken\)\.tableNames') - budget [commandTimeout](DiGi.PostgreSQL.md#DiGi.PostgreSQL.Query.EstimatedCountsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,bool,int,int,System.Threading.CancellationToken).commandTimeout 'DiGi\.PostgreSQL\.Query\.EstimatedCountsAsync\(this Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<string\>, bool, int, int, System\.Threading\.CancellationToken\)\.commandTimeout') accordingly.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.Dictionary<string,long>?> EstimatedCountsAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<string>? tableNames, bool analyze=false, int batchSize=1000, int commandTimeout=600, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.EstimatedCountsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,bool,int,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,bool,int,int,System.Threading.CancellationToken).tableNames'></a>

`tableNames` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The names of the tables to estimate\. Blank entries and duplicates are ignored\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,bool,int,int,System.Threading.CancellationToken).analyze'></a>

`analyze` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A boolean indicating whether to run VACUUM ANALYZE on each existing table before reading the estimates\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,bool,int,int,System.Threading.CancellationToken).batchSize'></a>

`batchSize` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The maximum number of table names sent in a single catalog query\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,bool,int,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds applied to every command executed\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Query.EstimatedCountsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,bool,int,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.Dictionary&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.dictionary-2 'System\.Collections\.Generic\.Dictionary\`2')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[,](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.dictionary-2 'System\.Collections\.Generic\.Dictionary\`2')[System\.Int64](https://learn.microsoft.com/en-us/dotnet/api/system.int64 'System\.Int64')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.dictionary-2 'System\.Collections\.Generic\.Dictionary\`2')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A dictionary keyed by table name holding the estimated row count for every table that exists, an empty dictionary when no usable name was supplied, or null when the connection or the names are null\.

<a name='DiGi.PostgreSQL.Query.HasRows(thisNpgsql.NpgsqlConnection,string)'></a>

## Query\.HasRows\(this NpgsqlConnection, string\) Method

Checks if a specified table in the PostgreSQL database contains any rows\.

```csharp
public static bool HasRows(this Npgsql.NpgsqlConnection? npgsqlConnection, string tableName);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.HasRows(thisNpgsql.NpgsqlConnection,string).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to execute the query\.

<a name='DiGi.PostgreSQL.Query.HasRows(thisNpgsql.NpgsqlConnection,string).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the table to check for existence of rows\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the table exists and contains at least one row; otherwise, false\.

<a name='DiGi.PostgreSQL.Query.IsAvailable(thisDiGi.PostgreSQL.Classes.ConnectionData)'></a>

## Query\.IsAvailable\(this ConnectionData\) Method

Checks if the PostgreSQL database is available using the provided connection data\.

```csharp
public static bool IsAvailable(this DiGi.PostgreSQL.Classes.ConnectionData connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.IsAvailable(thisDiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to establish a connection to the database\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the database is available and reachable; otherwise, false\.

<a name='DiGi.PostgreSQL.Query.IsAvailable(thisDiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile)'></a>

## Query\.IsAvailable\(this PostgreSQLConfigurationFile\) Method

Checks if the PostgreSQL database is available using the provided configuration file\.

```csharp
public static bool IsAvailable(this DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile postgreSQLConfigurationFile);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.IsAvailable(thisDiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile).postgreSQLConfigurationFile'></a>

`postgreSQLConfigurationFile` [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile')

The configuration file containing connection details\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the database is available and reachable; otherwise, false\.

<a name='DiGi.PostgreSQL.Query.PartitionAsync(thisNpgsql.NpgsqlConnection,short)'></a>

## Query\.PartitionAsync\(this NpgsqlConnection, short\) Method

Asynchronously retrieves a partition by its ID from the database\.

```csharp
public static System.Threading.Tasks.Task<DiGi.PostgreSQL.Classes.Partition?> PartitionAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, short partitionId);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.PartitionAsync(thisNpgsql.NpgsqlConnection,short).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection to use for the query\.

<a name='DiGi.PostgreSQL.Query.PartitionAsync(thisNpgsql.NpgsqlConnection,short).partitionId'></a>

`partitionId` [System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')

The unique identifier of the partition to retrieve\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition') object if found; otherwise, null\.

<a name='DiGi.PostgreSQL.Query.PartitionAsync(thisNpgsql.NpgsqlConnection,string)'></a>

## Query\.PartitionAsync\(this NpgsqlConnection, string\) Method

Asynchronously retrieves a partition by its name from the database\.

```csharp
public static System.Threading.Tasks.Task<DiGi.PostgreSQL.Classes.Partition?> PartitionAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, string? name);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.PartitionAsync(thisNpgsql.NpgsqlConnection,string).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection to use for the query\.

<a name='DiGi.PostgreSQL.Query.PartitionAsync(thisNpgsql.NpgsqlConnection,string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition to retrieve\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition') object if found; otherwise, null\.

<a name='DiGi.PostgreSQL.Query.PartitionIdAsync(thisNpgsql.NpgsqlConnection,string)'></a>

## Query\.PartitionIdAsync\(this NpgsqlConnection, string\) Method

Asynchronously retrieves the partition ID associated with the specified name\.

```csharp
public static System.Threading.Tasks.Task<System.Nullable<short>> PartitionIdAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, string? name);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.PartitionIdAsync(thisNpgsql.NpgsqlConnection,string).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection used to execute the query\.

<a name='DiGi.PostgreSQL.Query.PartitionIdAsync(thisNpgsql.NpgsqlConnection,string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition to retrieve the ID for\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the partition ID as a short if found; otherwise, null\.

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_,System.Threading.CancellationToken)'></a>

## Query\.PartitionsAsync\(this NpgsqlConnection, IEnumerable\<short\>, CancellationToken\) Method

Asynchronously retrieves partitions from the database based on a collection of partition identifiers\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Classes.Partition>?> PartitionsAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<short>? partitionIds, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection to use for the query\.

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_,System.Threading.CancellationToken).partitionIds'></a>

`partitionIds` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of short integers representing the IDs of the partitions to retrieve\.

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to cancel the asynchronous operation\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of matching partitions, or null if the connection or partitionIds is null\.

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,System.Threading.CancellationToken)'></a>

## Query\.PartitionsAsync\(this NpgsqlConnection, IEnumerable\<string\>, CancellationToken\) Method

Asynchronously retrieves partitions from the database based on a collection of partition names\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Classes.Partition>?> PartitionsAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<string>? names, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection to use for the query\.

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,System.Threading.CancellationToken).names'></a>

`names` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of strings representing the names of the partitions to retrieve\.

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to cancel the asynchronous operation\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of matching partitions, or null if the connection or names is null\.

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Threading.CancellationToken)'></a>

## Query\.PartitionsAsync\(this NpgsqlConnection, CancellationToken\) Method

Asynchronously retrieves all partitions from the database\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Classes.Partition>?> PartitionsAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection to use for the query\.

<a name='DiGi.PostgreSQL.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to cancel the asynchronous operation\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of partitions, or null if the connection is null\.

<a name='DiGi.PostgreSQL.Query.SerializableObjectAsync_USerializableObject_(Npgsql.NpgsqlDataReader,DiGi.PostgreSQL.Enums.DataType,int)'></a>

## Query\.SerializableObjectAsync\<USerializableObject\>\(NpgsqlDataReader, DataType, int\) Method

Asynchronously retrieves and deserializes a serializable object from the provided NpgsqlDataReader based on the specified data type\.

```csharp
public static System.Threading.Tasks.Task<USerializableObject?> SerializableObjectAsync<USerializableObject>(Npgsql.NpgsqlDataReader npgsqlDataReader, DiGi.PostgreSQL.Enums.DataType dataType, int index=0)
    where USerializableObject : DiGi.Core.Interfaces.ISerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Query.SerializableObjectAsync_USerializableObject_(Npgsql.NpgsqlDataReader,DiGi.PostgreSQL.Enums.DataType,int).USerializableObject'></a>

`USerializableObject`

The type of the serializable object to retrieve, which must implement ISerializableObject\.
#### Parameters

<a name='DiGi.PostgreSQL.Query.SerializableObjectAsync_USerializableObject_(Npgsql.NpgsqlDataReader,DiGi.PostgreSQL.Enums.DataType,int).npgsqlDataReader'></a>

`npgsqlDataReader` [Npgsql\.NpgsqlDataReader](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqldatareader 'Npgsql\.NpgsqlDataReader')

The NpgsqlDataReader containing the data to be read\.

<a name='DiGi.PostgreSQL.Query.SerializableObjectAsync_USerializableObject_(Npgsql.NpgsqlDataReader,DiGi.PostgreSQL.Enums.DataType,int).dataType'></a>

`dataType` [DataType](DiGi.PostgreSQL.Enums.md#DiGi.PostgreSQL.Enums.DataType 'DiGi\.PostgreSQL\.Enums\.DataType')

The data type of the value in the reader\.

<a name='DiGi.PostgreSQL.Query.SerializableObjectAsync_USerializableObject_(Npgsql.NpgsqlDataReader,DiGi.PostgreSQL.Enums.DataType,int).index'></a>

`index` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The zero\-based index of the column to read from\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[USerializableObject](DiGi.PostgreSQL.md#DiGi.PostgreSQL.Query.SerializableObjectAsync_USerializableObject_(Npgsql.NpgsqlDataReader,DiGi.PostgreSQL.Enums.DataType,int).USerializableObject 'DiGi\.PostgreSQL\.Query\.SerializableObjectAsync\<USerializableObject\>\(Npgsql\.NpgsqlDataReader, DiGi\.PostgreSQL\.Enums\.DataType, int\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the deserialized object if successful; otherwise, null\.

<a name='DiGi.PostgreSQL.Query.TableExistsAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,System.Threading.CancellationToken)'></a>

## Query\.TableExistsAsync\(this ConnectionData, string, CancellationToken\) Method

Checks if a table exists in the PostgreSQL database using the provided connection data\.

```csharp
public static System.Threading.Tasks.Task<bool> TableExistsAsync(this DiGi.PostgreSQL.Classes.ConnectionData? connectionData, string tableName, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.TableExistsAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,System.Threading.CancellationToken).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to create the Npgsql connection\.

<a name='DiGi.PostgreSQL.Query.TableExistsAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,System.Threading.CancellationToken).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the table to check for existence\.

<a name='DiGi.PostgreSQL.Query.TableExistsAsync(thisDiGi.PostgreSQL.Classes.ConnectionData,string,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains true if the table exists; otherwise, false\.

<a name='DiGi.PostgreSQL.Query.TableExistsAsync(thisNpgsql.NpgsqlConnection,string,System.Threading.CancellationToken)'></a>

## Query\.TableExistsAsync\(this NpgsqlConnection, string, CancellationToken\) Method

Checks if a table exists in the PostgreSQL database using the provided connection\.

```csharp
public static System.Threading.Tasks.Task<bool> TableExistsAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, string tableName, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.TableExistsAsync(thisNpgsql.NpgsqlConnection,string,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.Query.TableExistsAsync(thisNpgsql.NpgsqlConnection,string,System.Threading.CancellationToken).tableName'></a>

`tableName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the table to check for existence\.

<a name='DiGi.PostgreSQL.Query.TableExistsAsync(thisNpgsql.NpgsqlConnection,string,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains true if the table exists; otherwise, false\.

<a name='DiGi.PostgreSQL.Query.TableNames(thisDiGi.PostgreSQL.Classes.ConnectionData)'></a>

## Query\.TableNames\(this ConnectionData\) Method

Retrieves a list of table names from the public schema of the PostgreSQL database using provided connection data\.

```csharp
public static System.Collections.Generic.List<string>? TableNames(this DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.TableNames(thisDiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to establish a database connection\.

#### Returns
[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')  
A list of strings containing the table names, or null if the connection cannot be established\.

<a name='DiGi.PostgreSQL.Query.TableNames(thisNpgsql.NpgsqlConnection)'></a>

## Query\.TableNames\(this NpgsqlConnection\) Method

Retrieves a list of table names from the public schema of the PostgreSQL database\.

```csharp
public static System.Collections.Generic.List<string>? TableNames(this Npgsql.NpgsqlConnection? npgsqlConnection);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.TableNames(thisNpgsql.NpgsqlConnection).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to execute the query\.

#### Returns
[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')  
A list of strings containing the table names, or null if the connection is null\.

<a name='DiGi.PostgreSQL.Query.TryGetPostgreSQLDataType(thisstring,DiGi.PostgreSQL.Enums.PostgreSQLDataType)'></a>

## Query\.TryGetPostgreSQLDataType\(this string, PostgreSQLDataType\) Method

Attempts to get the corresponding PostgreSQL data type from a given string representation\.

```csharp
public static bool TryGetPostgreSQLDataType(this string value, out DiGi.PostgreSQL.Enums.PostgreSQLDataType postgreSQLDataType);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.TryGetPostgreSQLDataType(thisstring,DiGi.PostgreSQL.Enums.PostgreSQLDataType).value'></a>

`value` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The string value representing the data type\.

<a name='DiGi.PostgreSQL.Query.TryGetPostgreSQLDataType(thisstring,DiGi.PostgreSQL.Enums.PostgreSQLDataType).postgreSQLDataType'></a>

`postgreSQLDataType` [PostgreSQLDataType](DiGi.PostgreSQL.Enums.md#DiGi.PostgreSQL.Enums.PostgreSQLDataType 'DiGi\.PostgreSQL\.Enums\.PostgreSQLDataType')

When this method returns, contains the parsed PostgreSQL data type if successful; otherwise, PostgreSQLDataType\.Undefined\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the string was successfully converted to a PostgreSQL data type; otherwise, false\.

<a name='DiGi.PostgreSQL.Query.Type(thisDiGi.PostgreSQL.Enums.PostgreSQLDataType)'></a>

## Query\.Type\(this PostgreSQLDataType\) Method

Maps a PostgreSQL data type to its corresponding \.NET system type\.

```csharp
public static System.Type? Type(this DiGi.PostgreSQL.Enums.PostgreSQLDataType postgreSQLDataType);
```
#### Parameters

<a name='DiGi.PostgreSQL.Query.Type(thisDiGi.PostgreSQL.Enums.PostgreSQLDataType).postgreSQLDataType'></a>

`postgreSQLDataType` [PostgreSQLDataType](DiGi.PostgreSQL.Enums.md#DiGi.PostgreSQL.Enums.PostgreSQLDataType 'DiGi\.PostgreSQL\.Enums\.PostgreSQLDataType')

The PostgreSQL data type to convert\.

#### Returns
[System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')  
The corresponding \.NET [Type\(this PostgreSQLDataType\)](DiGi.PostgreSQL.md#DiGi.PostgreSQL.Query.Type(thisDiGi.PostgreSQL.Enums.PostgreSQLDataType) 'DiGi\.PostgreSQL\.Query\.Type\(this DiGi\.PostgreSQL\.Enums\.PostgreSQLDataType\)') if found; otherwise, `null`\.