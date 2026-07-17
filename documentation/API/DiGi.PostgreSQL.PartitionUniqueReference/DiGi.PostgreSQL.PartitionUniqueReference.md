#### [DiGi\.PostgreSQL\.PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Overview.md 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Overview')

## DiGi\.PostgreSQL\.PartitionUniqueReference Namespace
### Classes

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Create'></a>

## Create Class

```csharp
public static class Create
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Create
### Methods

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Create.PartitionUniqueReference(System.Collections.Generic.IReadOnlyList_string_)'></a>

## Create\.PartitionUniqueReference\(IReadOnlyList\<string\>\) Method

Rebuilds a [PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference') from the segments of its string form\.

```csharp
public static DiGi.Core.Interfaces.IReference? PartitionUniqueReference(System.Collections.Generic.IReadOnlyList<string?>? segments);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Create.PartitionUniqueReference(System.Collections.Generic.IReadOnlyList_string_).segments'></a>

`segments` [System\.Collections\.Generic\.IReadOnlyList&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ireadonlylist-1 'System\.Collections\.Generic\.IReadOnlyList\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ireadonlylist-1 'System\.Collections\.Generic\.IReadOnlyList\`1')

The segments: the partition name, then the nested unique reference\.

#### Returns
[DiGi\.Core\.Interfaces\.IReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.ireference 'DiGi\.Core\.Interfaces\.IReference')  
The reference, or `null` if the segments do not describe one\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify'></a>

## Modify Class

```csharp
public static class Modify
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Modify
### Methods

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.CleanTypesAsync(Npgsql.NpgsqlConnection)'></a>

## Modify\.CleanTypesAsync\(NpgsqlConnection\) Method

Cleans up unused types from the database by removing those that are not associated with any partition\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type>?> CleanTypesAsync(Npgsql.NpgsqlConnection? npgsqlConnection);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.CleanTypesAsync(Npgsql.NpgsqlConnection).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The connection to the PostgreSQL database\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A list of the types that were deleted, or null if the operation could not be performed\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_,bool)'></a>

## Modify\.RemoveAsync\(NpgsqlConnection, IEnumerable\<PartitionUniqueReference\>, bool\) Method

Removes the specified partition unique references from the database asynchronously\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference>?> RemoveAsync(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference> partitionUniqueReferences, bool clean=true);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_,bool).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The connection to the PostgreSQL database\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_,bool).partitionUniqueReferences'></a>

`partitionUniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of partition unique references to be removed\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_,bool).clean'></a>

`clean` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to perform cleanup of partitions and types after removal\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A HashSet of PartitionUniqueReference containing the successfully removed references, or `null` if the operation failed or no references were processed\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler)'></a>

## Modify\.UpdateAsync\<USerializableObject\>\(this NpgsqlConnection, IEnumerable\<USerializableObject\>, Func\<string,DataType\>, object, PartitionUniqueReferenceGeneratingEventHandler\) Method

Asynchronously updates the partition unique references in the PostgreSQL database for a collection of serializable objects\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference>?> UpdateAsync<USerializableObject>(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<USerializableObject> serializableObjects, System.Func<string?,DiGi.PostgreSQL.Enums.DataType> dataTypeFunc, object? sender=null, DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler? partitionUniqueReferenceGeneratingEventHandler=null)
    where USerializableObject : DiGi.Core.Interfaces.ISerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler).USerializableObject'></a>

`USerializableObject`

The type of the serializable object, which must implement [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection') used to communicate with the database\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler).serializableObjects'></a>

`serializableObjects` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[USerializableObject](DiGi.PostgreSQL.PartitionUniqueReference.md#DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler).USerializableObject 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Modify\.UpdateAsync\<USerializableObject\>\(this Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<USerializableObject\>, System\.Func\<string,DiGi\.PostgreSQL\.Enums\.DataType\>, object, DiGi\.PostgreSQL\.PartitionUniqueReference\.Delegates\.PartitionUniqueReferenceGeneratingEventHandler\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of objects that are to be updated in the database\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler).dataTypeFunc'></a>

`dataTypeFunc` [System\.Func&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[,](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')[DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')

A function used to determine the [DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType') based on a provided string key\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler).sender'></a>

`sender` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The object that initiated the request; defaults to null\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler).partitionUniqueReferenceGeneratingEventHandler'></a>

`partitionUniqueReferenceGeneratingEventHandler` [PartitionUniqueReferenceGeneratingEventHandler\(object, PartitionUniqueReferenceGeneratingEventArgs\)](DiGi.PostgreSQL.PartitionUniqueReference.Delegates.md#DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferenceGeneratingEventArgs) 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Delegates\.PartitionUniqueReferenceGeneratingEventHandler\(object, DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferenceGeneratingEventArgs\)')

An optional event handler for generating partition unique references; defaults to null\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task representing the asynchronous operation\. The task result contains a [System\.Collections\.Generic\.HashSet&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1') of updated [PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference') objects, or null if the connection is null, serializable objects are null, or critical table creation fails\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateTypeIdAsync(thisNpgsql.NpgsqlConnection,string)'></a>

## Modify\.UpdateTypeIdAsync\(this NpgsqlConnection, string\) Method

Updates or creates a type ID based on the provided name\.

```csharp
public static System.Threading.Tasks.Task<DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type?> UpdateTypeIdAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, string? name);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateTypeIdAsync(thisNpgsql.NpgsqlConnection,string).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance used to perform the operation\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Modify.UpdateTypeIdAsync(thisNpgsql.NpgsqlConnection,string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the type to update or create\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing the updated or created [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type') object, or null if the operation failed or inputs were invalid\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query'></a>

## Query Class

```csharp
public static class Query
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Query
### Methods

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition,System.Collections.Generic.IEnumerable_DiGi.Core.Interfaces.IUniqueReference_)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, Partition, IEnumerable\<IUniqueReference\>\) Method

Checks for the existence of multiple unique references within a specific database partition\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.Core.Interfaces.IUniqueReference>?> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, DiGi.PostgreSQL.Classes.Partition? partition, System.Collections.Generic.IEnumerable<DiGi.Core.Interfaces.IUniqueReference>? uniqueReferences);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition,System.Collections.Generic.IEnumerable_DiGi.Core.Interfaces.IUniqueReference_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition,System.Collections.Generic.IEnumerable_DiGi.Core.Interfaces.IUniqueReference_).partition'></a>

`partition` [DiGi\.PostgreSQL\.Classes\.Partition](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.partition 'DiGi\.PostgreSQL\.Classes\.Partition')

The partition to search within\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition,System.Collections.Generic.IEnumerable_DiGi.Core.Interfaces.IUniqueReference_).uniqueReferences'></a>

`uniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of unique references to verify\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing a set of existing unique references, or null if any input is null\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, Type\) Method

Checks whether a specific database type exists across all supported data type tables\.

```csharp
public static System.Threading.Tasks.Task<bool> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type? type);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type).type'></a>

`type` [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type')

The database type to check for existence\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing true if the type is found; otherwise, false\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, IEnumerable\<PartitionUniqueReference\>\) Method

Checks for the existence of a collection of partition unique references within the database\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference>?> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference> partitionUniqueReferences);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_).partitionUniqueReferences'></a>

`partitionUniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of partition unique references to verify\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing a set of existing partition unique references, or null if the input is invalid\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Type)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, Type\) Method

Checks whether a specific system type exists within the database\.

```csharp
public static System.Threading.Tasks.Task<bool> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, System.Type? type);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Type).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Type).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The system type to check for existence\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing true if the type is found; otherwise, false\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_)'></a>

## Query\.SerializableObjectsAsync\<USerializableObject\>\(NpgsqlConnection, IEnumerable\<PartitionUniqueReference\>\) Method

Asynchronously retrieves a list of serializable objects based on the provided PostgreSQL connection and partition unique references\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<USerializableObject>?> SerializableObjectsAsync<USerializableObject>(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference> partitionUniqueReferences)
    where USerializableObject : DiGi.Core.Interfaces.ISerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_).USerializableObject'></a>

`USerializableObject`

The type of serializable object to retrieve, which must implement ISerializableObject\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection used to execute the query\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_).partitionUniqueReferences'></a>

`partitionUniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of partition unique references to filter the objects\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[USerializableObject](DiGi.PostgreSQL.PartitionUniqueReference.md#DiGi.PostgreSQL.PartitionUniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_).USerializableObject 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Query\.SerializableObjectsAsync\<USerializableObject\>\(Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference\>\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing a list of serializable objects if successful; otherwise, null\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypeAsync(thisNpgsql.NpgsqlConnection,string)'></a>

## Query\.TypeAsync\(this NpgsqlConnection, string\) Method

Asynchronously retrieves a type from the database by its name\.

```csharp
public static System.Threading.Tasks.Task<DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type?> TypeAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, string? name);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypeAsync(thisNpgsql.NpgsqlConnection,string).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The connection to the PostgreSQL database\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypeAsync(thisNpgsql.NpgsqlConnection,string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the type to retrieve\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type') instance if the type is found; otherwise, `null`\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypeIdAsync(thisNpgsql.NpgsqlConnection,System.Type)'></a>

## Query\.TypeIdAsync\(this NpgsqlConnection, Type\) Method

Asynchronously retrieves the unique identifier of a type from the database based on its full name\.

```csharp
public static System.Threading.Tasks.Task<System.Nullable<short>> TypeIdAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Type? type);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypeIdAsync(thisNpgsql.NpgsqlConnection,System.Type).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection used to execute the query\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypeIdAsync(thisNpgsql.NpgsqlConnection,System.Type).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The system type for which the ID is being retrieved\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the type identifier as a short if found; otherwise, null\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypesAsync(thisNpgsql.NpgsqlConnection,string)'></a>

## Query\.TypesAsync\(this NpgsqlConnection, string\) Method

Asynchronously retrieves a list of types from the database based on the provided type name\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type>?> TypesAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, string? name);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypesAsync(thisNpgsql.NpgsqlConnection,string).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypesAsync(thisNpgsql.NpgsqlConnection,string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the type to search for\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of matching [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type') objects, or null if the connection is null or the name is invalid\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypesAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_)'></a>

## Query\.TypesAsync\(this NpgsqlConnection, IEnumerable\<short\>\) Method

Asynchronously retrieves a list of types from the database filtered by a collection of type identifiers\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type>?> TypesAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<short>? typeIds=null);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypesAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypesAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_short_).typeIds'></a>

`typeIds` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of short integers representing the IDs of the types to retrieve\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of matching [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type') objects, or null if the connection is null\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypesAsync(thisNpgsql.NpgsqlConnection,System.Type)'></a>

## Query\.TypesAsync\(this NpgsqlConnection, Type\) Method

Asynchronously retrieves a list of types from the database that are assignable from the specified type\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type>?> TypesAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Type? type);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypesAsync(thisNpgsql.NpgsqlConnection,System.Type).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.TypesAsync(thisNpgsql.NpgsqlConnection,System.Type).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The system type used to filter the retrieved types\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of matching [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type') objects, or null if the connection or type is null\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.UniqueTypeIdsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition)'></a>

## Query\.UniqueTypeIdsAsync\(this NpgsqlConnection, Partition\) Method

Retrieves all unique type IDs present in a specific partition from the database\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<short>?> UniqueTypeIdsAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, DiGi.PostgreSQL.Classes.Partition? partition);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.UniqueTypeIdsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection used to execute the query\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Query.UniqueTypeIdsAsync(thisNpgsql.NpgsqlConnection,DiGi.PostgreSQL.Classes.Partition).partition'></a>

`partition` [DiGi\.PostgreSQL\.Classes\.Partition](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.partition 'DiGi\.PostgreSQL\.Classes\.Partition')

The partition for which to retrieve the unique type IDs\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A [System\.Collections\.Generic\.HashSet&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1') containing the unique type IDs if successful; otherwise, `null`\.