#### [DiGi\.PostgreSQL\.UniqueReference](DiGi.PostgreSQL.UniqueReference.Overview.md 'DiGi\.PostgreSQL\.UniqueReference\.Overview')

## DiGi\.PostgreSQL\.UniqueReference Namespace
### Classes

<a name='DiGi.PostgreSQL.UniqueReference.Convert'></a>

## Convert Class

```csharp
public static class Convert
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Convert
### Methods

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject,TUniqueReference_(thisDiGi.PostgreSQL.Classes.ConnectionData,System.Collections.Generic.IEnumerable_TUniqueReference_)'></a>

## Convert\.ToDiGi\<TSerializableObject,TUniqueReference\>\(this ConnectionData, IEnumerable\<TUniqueReference\>\) Method

Converts the database objects with the specified unique references to a list of serializable objects of type [TSerializableObject](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject,TUniqueReference_(thisDiGi.PostgreSQL.Classes.ConnectionData,System.Collections.Generic.IEnumerable_TUniqueReference_).TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Convert\.ToDiGi\<TSerializableObject,TUniqueReference\>\(this DiGi\.PostgreSQL\.Classes\.ConnectionData, System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>\)\.TSerializableObject')\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<TSerializableObject>?> ToDiGi<TSerializableObject,TUniqueReference>(this DiGi.PostgreSQL.Classes.ConnectionData connectionData, System.Collections.Generic.IEnumerable<TUniqueReference> uniqueReferences)
    where TSerializableObject : DiGi.Core.Interfaces.ISerializableObject
    where TUniqueReference : DiGi.Core.Interfaces.IUniqueReference;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject,TUniqueReference_(thisDiGi.PostgreSQL.Classes.ConnectionData,System.Collections.Generic.IEnumerable_TUniqueReference_).TSerializableObject'></a>

`TSerializableObject`

The type of serializable object to convert to\.

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject,TUniqueReference_(thisDiGi.PostgreSQL.Classes.ConnectionData,System.Collections.Generic.IEnumerable_TUniqueReference_).TUniqueReference'></a>

`TUniqueReference`

The type of unique reference\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject,TUniqueReference_(thisDiGi.PostgreSQL.Classes.ConnectionData,System.Collections.Generic.IEnumerable_TUniqueReference_).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to connect to the database\.

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject,TUniqueReference_(thisDiGi.PostgreSQL.Classes.ConnectionData,System.Collections.Generic.IEnumerable_TUniqueReference_).uniqueReferences'></a>

`uniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject,TUniqueReference_(thisDiGi.PostgreSQL.Classes.ConnectionData,System.Collections.Generic.IEnumerable_TUniqueReference_).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Convert\.ToDiGi\<TSerializableObject,TUniqueReference\>\(this DiGi\.PostgreSQL\.Classes\.ConnectionData, System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of unique references to filter by\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[TSerializableObject](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject,TUniqueReference_(thisDiGi.PostgreSQL.Classes.ConnectionData,System.Collections.Generic.IEnumerable_TUniqueReference_).TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Convert\.ToDiGi\<TSerializableObject,TUniqueReference\>\(this DiGi\.PostgreSQL\.Classes\.ConnectionData, System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>\)\.TSerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task representing the asynchronous operation, returning the list of serializable objects or null\.

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject_(thisDiGi.PostgreSQL.Classes.ConnectionData,bool)'></a>

## Convert\.ToDiGi\<TSerializableObject\>\(this ConnectionData, bool\) Method

Converts the database objects to a list of serializable objects of type [TSerializableObject](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject_(thisDiGi.PostgreSQL.Classes.ConnectionData,bool).TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Convert\.ToDiGi\<TSerializableObject\>\(this DiGi\.PostgreSQL\.Classes\.ConnectionData, bool\)\.TSerializableObject')\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<TSerializableObject>?> ToDiGi<TSerializableObject>(this DiGi.PostgreSQL.Classes.ConnectionData connectionData, bool inheritance=true)
    where TSerializableObject : DiGi.Core.Interfaces.ISerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject_(thisDiGi.PostgreSQL.Classes.ConnectionData,bool).TSerializableObject'></a>

`TSerializableObject`

The type of serializable object to convert to\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject_(thisDiGi.PostgreSQL.Classes.ConnectionData,bool).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to connect to the database\.

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject_(thisDiGi.PostgreSQL.Classes.ConnectionData,bool).inheritance'></a>

`inheritance` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to include inherited types\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[TSerializableObject](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Convert.ToDiGi_TSerializableObject_(thisDiGi.PostgreSQL.Classes.ConnectionData,bool).TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Convert\.ToDiGi\<TSerializableObject\>\(this DiGi\.PostgreSQL\.Classes\.ConnectionData, bool\)\.TSerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task representing the asynchronous operation, returning the list of serializable objects or null\.

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToPostgreSQL(thisDiGi.Core.Interfaces.ISerializableObject,DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## Convert\.ToPostgreSQL\(this ISerializableObject, ConnectionData\) Method

Converts a serializable object to its PostgreSQL unique reference representation\.

```csharp
public static System.Threading.Tasks.Task<DiGi.Core.Classes.UniqueReference?> ToPostgreSQL(this DiGi.Core.Interfaces.ISerializableObject? serializableObject, DiGi.PostgreSQL.Classes.ConnectionData connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToPostgreSQL(thisDiGi.Core.Interfaces.ISerializableObject,DiGi.PostgreSQL.Classes.ConnectionData).serializableObject'></a>

`serializableObject` [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')

The serializable object to convert\.

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToPostgreSQL(thisDiGi.Core.Interfaces.ISerializableObject,DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data for the PostgreSQL database\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[DiGi\.Core\.Classes\.UniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.uniquereference 'DiGi\.Core\.Classes\.UniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing the converted unique reference or null if the input object is null\.

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToPostgreSQL_TSerializableObject_(thisSystem.Collections.Generic.IEnumerable_TSerializableObject_,DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## Convert\.ToPostgreSQL\<TSerializableObject\>\(this IEnumerable\<TSerializableObject\>, ConnectionData\) Method

Converts a collection of serializable objects to their PostgreSQL unique reference representations\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.Core.Classes.UniqueReference>?> ToPostgreSQL<TSerializableObject>(this System.Collections.Generic.IEnumerable<TSerializableObject>? serializableObjects, DiGi.PostgreSQL.Classes.ConnectionData? connectionData)
    where TSerializableObject : DiGi.Core.Interfaces.ISerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToPostgreSQL_TSerializableObject_(thisSystem.Collections.Generic.IEnumerable_TSerializableObject_,DiGi.PostgreSQL.Classes.ConnectionData).TSerializableObject'></a>

`TSerializableObject`

The type of the serializable objects, which must implement ISerializableObject\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToPostgreSQL_TSerializableObject_(thisSystem.Collections.Generic.IEnumerable_TSerializableObject_,DiGi.PostgreSQL.Classes.ConnectionData).serializableObjects'></a>

`serializableObjects` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[TSerializableObject](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Convert.ToPostgreSQL_TSerializableObject_(thisSystem.Collections.Generic.IEnumerable_TSerializableObject_,DiGi.PostgreSQL.Classes.ConnectionData).TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Convert\.ToPostgreSQL\<TSerializableObject\>\(this System\.Collections\.Generic\.IEnumerable\<TSerializableObject\>, DiGi\.PostgreSQL\.Classes\.ConnectionData\)\.TSerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of serializable objects to convert\.

<a name='DiGi.PostgreSQL.UniqueReference.Convert.ToPostgreSQL_TSerializableObject_(thisSystem.Collections.Generic.IEnumerable_TSerializableObject_,DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data for the PostgreSQL database\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[DiGi\.Core\.Classes\.UniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.uniquereference 'DiGi\.Core\.Classes\.UniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing a hash set of converted unique references or null if inputs are null\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify'></a>

## Modify Class

```csharp
public static class Modify
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Modify
### Methods

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken)'></a>

## Modify\.RemoveAsync\(NpgsqlConnection, Type, bool, int, CancellationToken\) Method

Removes all unique references associated with a specific type from the database asynchronously\.

```csharp
public static System.Threading.Tasks.Task<bool> RemoveAsync(Npgsql.NpgsqlConnection? npgsqlConnection, System.Type type, bool inheritance=true, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to be used for the operation\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The type whose references should be removed\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken).inheritance'></a>

`inheritance` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to include inherited types in the removal process\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync(Npgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
True if the removal was successful; otherwise, false\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync_TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,bool,int,System.Threading.CancellationToken)'></a>

## Modify\.RemoveAsync\<TUniqueReference\>\(NpgsqlConnection, IEnumerable\<TUniqueReference\>, bool, int, CancellationToken\) Method

Removes a collection of unique references from the database asynchronously\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<TUniqueReference>?> RemoveAsync<TUniqueReference>(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<TUniqueReference> uniqueReferences, bool clean=true, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TUniqueReference : DiGi.Core.Interfaces.IUniqueReference;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync_TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,bool,int,System.Threading.CancellationToken).TUniqueReference'></a>

`TUniqueReference`

The type of the unique reference, which must implement IUniqueReference\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync_TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,bool,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to be used for the operation\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync_TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,bool,int,System.Threading.CancellationToken).uniqueReferences'></a>

`uniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync_TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,bool,int,System.Threading.CancellationToken).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Modify\.RemoveAsync\<TUniqueReference\>\(Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>, bool, int, System\.Threading\.CancellationToken\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of unique references to remove\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync_TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,bool,int,System.Threading.CancellationToken).clean'></a>

`clean` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to clean partitions after the removal process\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync_TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,bool,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync_TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,bool,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Modify.RemoveAsync_TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,bool,int,System.Threading.CancellationToken).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Modify\.RemoveAsync\<TUniqueReference\>\(Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>, bool, int, System\.Threading\.CancellationToken\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A list of successfully removed unique references, or null if the connection is null or no valid references were found\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_System.Type,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler,int,System.Threading.CancellationToken)'></a>

## Modify\.UpdateAsync\<USerializableObject\>\(this NpgsqlConnection, IEnumerable\<USerializableObject\>, Func\<Type,DataType\>, object, UniqueIdReferenceGeneratingEventHandler, int, CancellationToken\) Method

Asynchronously updates or inserts serializable objects into the PostgreSQL database, utilizing partitioning and unique references via a batch operation\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.Core.Classes.UniqueReference>?> UpdateAsync<USerializableObject>(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<USerializableObject> serializableObjects, System.Func<System.Type?,DiGi.PostgreSQL.Enums.DataType> dataTypeFunc, object? sender=null, DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler? uniqueIdReferenceGeneratingEventHandler=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where USerializableObject : DiGi.Core.Interfaces.ISerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_System.Type,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).USerializableObject'></a>

`USerializableObject`

The type of object that implements [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_System.Type,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection') used to connect to the PostgreSQL database\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_System.Type,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).serializableObjects'></a>

`serializableObjects` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[USerializableObject](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_System.Type,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).USerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Modify\.UpdateAsync\<USerializableObject\>\(this Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<USerializableObject\>, System\.Func\<System\.Type,DiGi\.PostgreSQL\.Enums\.DataType\>, object, DiGi\.PostgreSQL\.UniqueReference\.Delegates\.UniqueIdReferenceGeneratingEventHandler, int, System\.Threading\.CancellationToken\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An [System\.Collections\.Generic\.IEnumerable&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1') containing the objects to be updated or inserted\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_System.Type,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).dataTypeFunc'></a>

`dataTypeFunc` [System\.Func&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')[System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')[,](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')[DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.func-2 'System\.Func\`2')

A delegate that maps a [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type') to a [DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType')\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_System.Type,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).sender'></a>

`sender` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The source of the event, used when invoking the unique ID reference generating event handler\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_System.Type,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).uniqueIdReferenceGeneratingEventHandler'></a>

`uniqueIdReferenceGeneratingEventHandler` [UniqueIdReferenceGeneratingEventHandler\(object, UniqueIdReferenceGeneratingEventArgs\)](DiGi.PostgreSQL.UniqueReference.Delegates.md#DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs) 'DiGi\.PostgreSQL\.UniqueReference\.Delegates\.UniqueIdReferenceGeneratingEventHandler\(object, DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueIdReferenceGeneratingEventArgs\)')

An optional [UniqueIdReferenceGeneratingEventHandler\(object, UniqueIdReferenceGeneratingEventArgs\)](DiGi.PostgreSQL.UniqueReference.Delegates.md#DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs) 'DiGi\.PostgreSQL\.UniqueReference\.Delegates\.UniqueIdReferenceGeneratingEventHandler\(object, DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueIdReferenceGeneratingEventArgs\)') to customize the generation of unique references\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_System.Type,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Modify.UpdateAsync_USerializableObject_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_USerializableObject_,System.Func_System.Type,DiGi.PostgreSQL.Enums.DataType_,object,DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[DiGi\.Core\.Classes\.UniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.uniquereference 'DiGi\.Core\.Classes\.UniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a HashSet of UniqueReference of updated references, or null if the connection or objects are null or an error occurs during table creation\.

<a name='DiGi.PostgreSQL.UniqueReference.Query'></a>

## Query Class

```csharp
public static class Query
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → Query
### Methods

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken)'></a>

## Query\.ContainsAsync\(this NpgsqlConnection, Type, int, CancellationToken\) Method

Checks whether the specified type exists in the database\.

```csharp
public static System.Threading.Tasks.Task<bool> ContainsAsync(this Npgsql.NpgsqlConnection npgsqlConnection, System.Type? type, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The type to check for existence\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation, containing true if the type exists; otherwise, false\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync_TUniqueReference_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken)'></a>

## Query\.ContainsAsync\<TUniqueReference\>\(this NpgsqlConnection, IEnumerable\<TUniqueReference\>, int, CancellationToken\) Method

Checks whether the specified unique references exist in the database\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.HashSet<TUniqueReference>?> ContainsAsync<TUniqueReference>(this Npgsql.NpgsqlConnection npgsqlConnection, System.Collections.Generic.IEnumerable<TUniqueReference>? uniqueReferences, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TUniqueReference : DiGi.Core.Interfaces.IUniqueReference;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync_TUniqueReference_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).TUniqueReference'></a>

`TUniqueReference`

The type of the unique reference, which must implement [DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync_TUniqueReference_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync_TUniqueReference_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).uniqueReferences'></a>

`uniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync_TUniqueReference_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Query\.ContainsAsync\<TUniqueReference\>\(this Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>, int, System\.Threading\.CancellationToken\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of unique references to check for existence\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync_TUniqueReference_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync_TUniqueReference_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Query.ContainsAsync_TUniqueReference_(thisNpgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Query\.ContainsAsync\<TUniqueReference\>\(this Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>, int, System\.Threading\.CancellationToken\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A hash set containing the unique references that were found in the database, or null if the connection or input collection is null\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken)'></a>

## Query\.CountAsync\(this NpgsqlConnection, Type, bool, int, CancellationToken\) Method

Asynchronously counts the number of records for a specified type in the database\.

```csharp
public static System.Threading.Tasks.Task<long> CountAsync(this Npgsql.NpgsqlConnection npgsqlConnection, System.Type? type, bool inheritance=true, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection instance used to execute the query\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The type of the objects to count\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken).inheritance'></a>

`inheritance` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to include inherited types in the count\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.CountAsync(thisNpgsql.NpgsqlConnection,System.Type,bool,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Int64](https://learn.microsoft.com/en-us/dotnet/api/system.int64 'System\.Int64')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the total record count, or \-1 if the connection or type is null\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionIdAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken)'></a>

## Query\.PartitionIdAsync\(this NpgsqlConnection, Type, int, CancellationToken\) Method

Asynchronously retrieves the partition identifier for a given type using the provided PostgreSQL connection\.

```csharp
public static System.Threading.Tasks.Task<System.Nullable<short>> PartitionIdAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Type? type, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionIdAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to use for the query\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionIdAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The type for which the partition ID is being retrieved\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionIdAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionIdAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the partition identifier as a short, or null if not found or inputs are invalid\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken)'></a>

## Query\.PartitionsAsync\(this NpgsqlConnection, string, int, CancellationToken\) Method

Asynchronously retrieves a list of partitions associated with the specified type name\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Classes.Partition>?> PartitionsAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, string? name, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection to use for the query\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the type used to filter the partitions\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,string,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[DiGi\.PostgreSQL\.Classes\.Partition](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.partition 'DiGi\.PostgreSQL\.Classes\.Partition')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [DiGi\.PostgreSQL\.Classes\.Partition](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.partition 'DiGi\.PostgreSQL\.Classes\.Partition') objects, or null if the connection is null or the name is invalid\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken)'></a>

## Query\.PartitionsAsync\(this NpgsqlConnection, Type, int, CancellationToken\) Method

Asynchronously retrieves a list of partitions that are assignable from the specified type\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Classes.Partition>?> PartitionsAsync(this Npgsql.NpgsqlConnection? npgsqlConnection, System.Type? type, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection to use for the query\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The system type used to filter the partitions\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.PartitionsAsync(thisNpgsql.NpgsqlConnection,System.Type,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[DiGi\.PostgreSQL\.Classes\.Partition](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.partition 'DiGi\.PostgreSQL\.Classes\.Partition')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [DiGi\.PostgreSQL\.Classes\.Partition](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.partition 'DiGi\.PostgreSQL\.Classes\.Partition') objects, or null if the connection or type is null\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject,TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken)'></a>

## Query\.SerializableObjectsAsync\<USerializableObject,TUniqueReference\>\(NpgsqlConnection, IEnumerable\<TUniqueReference\>, int, CancellationToken\) Method

Asynchronously retrieves a list of serializable objects from the database using a collection of unique references\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<USerializableObject>?> SerializableObjectsAsync<USerializableObject,TUniqueReference>(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<TUniqueReference> uniqueReferences, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where USerializableObject : DiGi.Core.Interfaces.ISerializableObject
    where TUniqueReference : DiGi.Core.Interfaces.IUniqueReference;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject,TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).USerializableObject'></a>

`USerializableObject`

The type of serializable object to retrieve, which must implement [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject,TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).TUniqueReference'></a>

`TUniqueReference`

The type of the unique reference used for lookup, which must implement [DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject,TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection used to execute the database query\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject,TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).uniqueReferences'></a>

`uniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject,TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Query\.SerializableObjectsAsync\<USerializableObject,TUniqueReference\>\(Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>, int, System\.Threading\.CancellationToken\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of unique references to be resolved into serializable objects\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject,TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject,TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[USerializableObject](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject,TUniqueReference_(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_TUniqueReference_,int,System.Threading.CancellationToken).USerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Query\.SerializableObjectsAsync\<USerializableObject,TUniqueReference\>\(Npgsql\.NpgsqlConnection, System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>, int, System\.Threading\.CancellationToken\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of retrieved serializable objects, or null if the connection or unique references are null\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,bool,int,System.Threading.CancellationToken)'></a>

## Query\.SerializableObjectsAsync\<USerializableObject\>\(NpgsqlConnection, bool, int, CancellationToken\) Method

Asynchronously retrieves a list of serializable objects from the database based on the specified type and inheritance settings\.

```csharp
public static System.Threading.Tasks.Task<System.Collections.Generic.List<USerializableObject>?> SerializableObjectsAsync<USerializableObject>(Npgsql.NpgsqlConnection? npgsqlConnection, bool inheritance=true, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where USerializableObject : DiGi.Core.Interfaces.ISerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,bool,int,System.Threading.CancellationToken).USerializableObject'></a>

`USerializableObject`

The type of serializable object to retrieve, which must implement [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,bool,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection used to execute the database query\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,bool,int,System.Threading.CancellationToken).inheritance'></a>

`inheritance` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to include inherited types in the retrieval process\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,bool,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,bool,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

A token to monitor for cancellation requests\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[USerializableObject](DiGi.PostgreSQL.UniqueReference.md#DiGi.PostgreSQL.UniqueReference.Query.SerializableObjectsAsync_USerializableObject_(Npgsql.NpgsqlConnection,bool,int,System.Threading.CancellationToken).USerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Query\.SerializableObjectsAsync\<USerializableObject\>\(Npgsql\.NpgsqlConnection, bool, int, System\.Threading\.CancellationToken\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of retrieved serializable objects, or null if the connection is null\.