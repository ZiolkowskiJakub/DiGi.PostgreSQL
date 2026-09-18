#### [DiGi\.PostgreSQL\.PartitionReference](DiGi.PostgreSQL.PartitionReference.Overview.md 'DiGi\.PostgreSQL\.PartitionReference\.Overview')

## DiGi\.PostgreSQL\.PartitionReference Namespace
### Classes

<a name='DiGi.PostgreSQL.PartitionReference.Create'></a>

## Create Class

```csharp
public static class Create
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Create
### Methods

<a name='DiGi.PostgreSQL.PartitionReference.Create.PartitionReference(System.Collections.Generic.IReadOnlyList_string_)'></a>

## Create\.PartitionReference\(IReadOnlyList\<string\>\) Method

Rebuilds a [PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference') from the segments of its string form\.

```csharp
public static DiGi.Core.Interfaces.IReference? PartitionReference(System.Collections.Generic.IReadOnlyList<string?>? segments);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Create.PartitionReference(System.Collections.Generic.IReadOnlyList_string_).segments'></a>

`segments` [System\.Collections\.Generic\.IReadOnlyList&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ireadonlylist-1 'System\.Collections\.Generic\.IReadOnlyList\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ireadonlylist-1 'System\.Collections\.Generic\.IReadOnlyList\`1')

The segments: the partition name, then the unique identifier\.

#### Returns
[DiGi\.Core\.Interfaces\.IReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.ireference 'DiGi\.Core\.Interfaces\.IReference')  
The reference, or `null` if the segments do not describe one\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify'></a>

## Modify Class

```csharp
public static class Modify
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Modify
### Methods

<a name='DiGi.PostgreSQL.PartitionReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken)'></a>

## Modify\.RemoveAsync\(NpgsqlConnection, IEnumerable\<PartitionReference\>, int, CancellationToken\) Method

Asynchronously removes the specified partition references from the database\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference>?> RemoveAsync(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference> partitionReferences, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to be used for the operation\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).partitionReferences'></a>

`partitionReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of partition references to remove\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A hash set containing the successfully removed partition references, or null if the connection is null or an error occurred during processing\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken)'></a>

## Modify\.UpdateAsync\<USerializableObject\>\(this NpgsqlConnection, IEnumerable\<USerializableObject\>, Func\<string,DataType\>, object, PartitionReferenceGeneratingEventHandler, int, CancellationToken\) Method

Asynchronously updates the specified serializable objects in the PostgreSQL database, utilizing partitions and UPSERT logic\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference>?> UpdateAsync<USerializableObject>(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<USerializableObject> serializableObjects, System.Func<string?,DiGi.PostgreSQL.Enums.DataType> dataTypeFunc, object? sender=null, DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler? partitionReferenceGeneratingEventHandler=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where USerializableObject : DiGi.Core.Interfaces.ISerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).USerializableObject'></a>

`USerializableObject`

The type of the serializable object, which must implement [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to be used for the database operation\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).serializableObjects'></a>

`serializableObjects` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[USerializableObject](DiGi.PostgreSQL.PartitionReference.md#DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).USerializableObject 'DiGi\.PostgreSQL\.PartitionReference\.Modify\.UpdateAsync\<USerializableObject\>\(this Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<USerializableObject\>, System\.Func\<string,DiGi\.PostgreSQL\.Enums\.DataType\>, object, DiGi\.PostgreSQL\.PartitionReference\.Delegates\.PartitionReferenceGeneratingEventHandler, int, System\.Threading\.CancellationToken\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of objects to be updated\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).dataTypeFunc'></a>

`dataTypeFunc` [System\.Func&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[,](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')[DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')

A function that determines the [DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType') based on the partition name\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).sender'></a>

`sender` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The object that sends the event, passed to the [partitionReferenceGeneratingEventHandler](DiGi.PostgreSQL.PartitionReference.md#DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).partitionReferenceGeneratingEventHandler 'DiGi\.PostgreSQL\.PartitionReference\.Modify\.UpdateAsync\<USerializableObject\>\(this Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<USerializableObject\>, System\.Func\<string,DiGi\.PostgreSQL\.Enums\.DataType\>, object, DiGi\.PostgreSQL\.PartitionReference\.Delegates\.PartitionReferenceGeneratingEventHandler, int, System\.Threading\.CancellationToken\)\.partitionReferenceGeneratingEventHandler')\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).partitionReferenceGeneratingEventHandler'></a>

`partitionReferenceGeneratingEventHandler` [PartitionReferenceGeneratingEventHandler\(object, PartitionReferenceGeneratingEventArgs\)](DiGi.PostgreSQL.PartitionReference.Delegates.md#DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs) 'DiGi\.PostgreSQL\.PartitionReference\.Delegates\.PartitionReferenceGeneratingEventHandler\(object, DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferenceGeneratingEventArgs\)')

An optional event handler for generating partition references\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.PartitionReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_string,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a HashSet of PartitionReference of updated partition references, or null if the connection or objects are null or if the table creation fails\.

<a name='DiGi.PostgreSQL.PartitionReference.Query'></a>

## Query Class

```csharp
public static class Query
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Query
### Methods

<a name='DiGi.PostgreSQL.PartitionReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, IEnumerable\<PartitionReference\>, int, CancellationToken\) Method

Asynchronously checks if the specified partition references exist in the database\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference>?> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference>? partitionReferences, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to be used for the operation\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).partitionReferences'></a>

`partitionReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of partition references to check for existence\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a HashSet of the partition references that exist in the database, or null if the connection or the input collection is null\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken)'></a>

## Query\.SerializableObjectsAsync\<USerializableObject\>\(NpgsqlConnection, string, int, CancellationToken\) Method

Asynchronously retrieves a list of serializable objects associated with a specific partition name\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<USerializableObject>?> SerializableObjectsAsync<USerializableObject>(Npgsql.NpgsqlConnection? npgsqlConnection, string name, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where USerializableObject : DiGi.Core.Interfaces.ISerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).USerializableObject'></a>

`USerializableObject`

The type of the serializable object, which must implement ISerializableObject\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the database query\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition from which to retrieve objects\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[USerializableObject](DiGi.PostgreSQL.PartitionReference.md#DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).USerializableObject 'DiGi\.PostgreSQL\.PartitionReference\.Query\.SerializableObjectsAsync\<USerializableObject\>\(Npgsql\.NpgsqlConnection, string, int, System\.Threading\.CancellationToken\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing a list of serializable objects or null if the connection is null\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken)'></a>

## Query\.SerializableObjectsAsync\<USerializableObject\>\(NpgsqlConnection, IEnumerable\<PartitionReference\>, int, CancellationToken\) Method

Asynchronously retrieves a list of serializable objects associated with a collection of partition references\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<USerializableObject>?> SerializableObjectsAsync<USerializableObject>(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference> partitionReferences, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where USerializableObject : DiGi.Core.Interfaces.ISerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).USerializableObject'></a>

`USerializableObject`

The type of the serializable object, which must implement ISerializableObject\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the database query\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).partitionReferences'></a>

`partitionReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of partition references used to identify the objects to retrieve\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[USerializableObject](DiGi.PostgreSQL.PartitionReference.md#DiGi.PostgreSQL.PartitionReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_,int,System.Threading.CancellationToken).USerializableObject 'DiGi\.PostgreSQL\.PartitionReference\.Query\.SerializableObjectsAsync\<USerializableObject\>\(Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference\>, int, System\.Threading\.CancellationToken\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing a list of serializable objects or null if the connection or partition references are null\.