#### [DiGi\.PostgreSQL\.UniqueReference](DiGi.PostgreSQL.UniqueReference.Overview.md 'DiGi\.PostgreSQL\.UniqueReference\.Overview')

## DiGi\.PostgreSQL\.UniqueReference\.Classes Namespace
### Classes

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs'></a>

## UniqueIdReferenceGeneratingEventArgs Class

Event arguments used during the generation of a unique ID reference\.

```csharp
public class UniqueIdReferenceGeneratingEventArgs : DiGi.PostgreSQL.Classes.ReferenceGeneratingEventArgs
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [System\.EventArgs](https://learn.microsoft.com/en-us/dotnet/api/system.eventargs 'System\.EventArgs') → [DiGi\.PostgreSQL\.Classes\.ReferenceGeneratingEventArgs](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.referencegeneratingeventargs 'DiGi\.PostgreSQL\.Classes\.ReferenceGeneratingEventArgs') → UniqueIdReferenceGeneratingEventArgs
### Constructors

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs.UniqueIdReferenceGeneratingEventArgs(object)'></a>

## UniqueIdReferenceGeneratingEventArgs\(object\) Constructor

Initializes a new instance of the [UniqueIdReferenceGeneratingEventArgs](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueIdReferenceGeneratingEventArgs') class\.

```csharp
public UniqueIdReferenceGeneratingEventArgs(object? item);
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs.UniqueIdReferenceGeneratingEventArgs(object).item'></a>

`item` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The item for which the reference is being generated\.
### Properties

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs.UniqueId'></a>

## UniqueIdReferenceGeneratingEventArgs\.UniqueId Property

Gets or sets the unique identifier string associated with this reference\.

```csharp
public string? UniqueId { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs.UniqueIdReference'></a>

## UniqueIdReferenceGeneratingEventArgs\.UniqueIdReference Property

Gets the constructed [UniqueIdReference](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs.UniqueIdReference 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueIdReferenceGeneratingEventArgs\.UniqueIdReference') based on the item type and the provided unique identifier\.

```csharp
public DiGi.Core.Classes.UniqueIdReference? UniqueIdReference { get; }
```

#### Property Value
[DiGi\.Core\.Classes\.UniqueIdReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.uniqueidreference 'DiGi\.Core\.Classes\.UniqueIdReference')

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter'></a>

## UniqueReferencePostgreSQLConverter Class

Provides a specialized PostgreSQL converter for unique references specifically targeting objects that implement the [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject') interface\.

```csharp
public class UniqueReferencePostgreSQLConverter : DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter<DiGi.Core.Interfaces.ISerializableObject>
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1') → [DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter&lt;](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_ 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_ 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>') → UniqueReferencePostgreSQLConverter
### Constructors

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter.UniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## UniqueReferencePostgreSQLConverter\(ConnectionData\) Constructor

Initializes a new instance of the [UniqueReferencePostgreSQLConverter](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter') class using the specified connection data\.

```csharp
public UniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter.UniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to configure the PostgreSQL converter; may be null\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\> Class

Provides a PostgreSQL converter implementation specifically designed to handle unique references for objects that implement the [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject') interface\.

```csharp
public class UniqueReferencePostgreSQLConverter<TSerializableObject> : DiGi.PostgreSQL.Classes.PostgreSQLConverter<TSerializableObject>
    where TSerializableObject : DiGi.Core.Interfaces.ISerializableObject
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject'></a>

`TSerializableObject`

The type of the serializable object being converted, which must implement the [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject') interface\.

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1')[TSerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1') → UniqueReferencePostgreSQLConverter\<TSerializableObject\>

Derived  
↳ [UniqueReferencePostgreSQLConverter](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter')
### Constructors

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.UniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## UniqueReferencePostgreSQLConverter\(ConnectionData\) Constructor

Initializes a new instance of the [UniqueReferencePostgreSQLConverter](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter') class\.

```csharp
public UniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.UniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to configure the PostgreSQL database connection; can be null\.
### Methods

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync(System.Type)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.ContainsAsync\(Type\) Method

Asynchronously determines whether the container contains the specified type\.

```csharp
public System.Threading.Tasks.Task<bool> ContainsAsync(System.Type? type);
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync(System.Type).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type') to locate in the container\. This value can be null\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool') if the specified type is found; otherwise, [false](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.ContainsAsync\<TUniqueReference\>\(IEnumerable\<TUniqueReference\>\) Method

Asynchronously checks for the existence of a collection of unique references and returns those that are present\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<TUniqueReference>?> ContainsAsync<TUniqueReference>(System.Collections.Generic.IEnumerable<TUniqueReference>? uniqueReferences)
    where TUniqueReference : DiGi.Core.Interfaces.IUniqueReference;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).TUniqueReference'></a>

`TUniqueReference`

The type of the unique reference, which must implement the [DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference') interface\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).uniqueReferences'></a>

`uniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.ContainsAsync\<TUniqueReference\>\(System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional enumerable collection of unique references to verify\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.ContainsAsync\<TUniqueReference\>\(System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a [System\.Collections\.Generic\.HashSet&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1') of existing references, or [null](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/null 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/keywords/null') if no references were provided or found\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.CountAsync(System.Type,bool)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.CountAsync\(Type, bool\) Method

Asynchronously counts the number of elements associated with the specified type\.

```csharp
public System.Threading.Tasks.Task<long> CountAsync(System.Type? type, bool inheritance=true);
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.CountAsync(System.Type,bool).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type') to count\. If null, the behavior is determined by the underlying implementation\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.CountAsync(System.Type,bool).inheritance'></a>

`inheritance` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to include types derived from the specified type in the count\. Defaults to `true`\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Int64](https://learn.microsoft.com/en-us/dotnet/api/system.int64 'System\.Int64')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the total count as a [System\.Int64](https://learn.microsoft.com/en-us/dotnet/api/system.int64 'System\.Int64')\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.CountAsync_USerializableObject_(bool)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.CountAsync\<USerializableObject\>\(bool\) Method

Asynchronously counts the total number of records for the specified serializable object type\.

```csharp
public System.Threading.Tasks.Task<long> CountAsync<USerializableObject>(bool inheritance=true)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.CountAsync_USerializableObject_(bool).USerializableObject'></a>

`USerializableObject`

The type of the serializable object to count, which must derive from [TSerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.CountAsync_USerializableObject_(bool).inheritance'></a>

`inheritance` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to include types derived from [USerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.CountAsync_USerializableObject_(bool).USerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.CountAsync\<USerializableObject\>\(bool\)\.USerializableObject') in the count\. Defaults to [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Int64](https://learn.microsoft.com/en-us/dotnet/api/system.int64 'System\.Int64')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the total number of records as a [System\.Int64](https://learn.microsoft.com/en-us/dotnet/api/system.int64 'System\.Int64')\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetDataType(System.Type)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetDataType\(Type\) Method

Retrieves the corresponding [DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType') for the specified \.NET type\.

```csharp
public virtual DiGi.PostgreSQL.Enums.DataType GetDataType(System.Type? type);
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetDataType(System.Type).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The \.NET type to map to a [DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType')\. This value can be null\.

#### Returns
[DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType')  
The [DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType') that represents the provided type\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetDataType_USerializableObject_()'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetDataType\<USerializableObject\>\(\) Method

Retrieves the [DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType') associated with the specified serializable object type\.

```csharp
public DiGi.PostgreSQL.Enums.DataType GetDataType<USerializableObject>()
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetDataType_USerializableObject_().USerializableObject'></a>

`USerializableObject`

The type of the serializable object, which must derive from [TSerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')\.

#### Returns
[DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType')  
The [DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType') corresponding to the provided generic type\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.Core.Interfaces.IUniqueReference)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectAsync\<USerializableObject\>\(IUniqueReference\) Method

Asynchronously retrieves a serializable object associated with the specified unique reference\.

```csharp
public System.Threading.Tasks.Task<USerializableObject?> GetSerializableObjectAsync<USerializableObject>(DiGi.Core.Interfaces.IUniqueReference? uniqueReference)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.Core.Interfaces.IUniqueReference).USerializableObject'></a>

`USerializableObject`

The type of the serializable object to retrieve, which must derive from [TSerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.Core.Interfaces.IUniqueReference).uniqueReference'></a>

`uniqueReference` [DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference')

The unique reference used to identify the object\. May be null\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[USerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.Core.Interfaces.IUniqueReference).USerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectAsync\<USerializableObject\>\(DiGi\.Core\.Interfaces\.IUniqueReference\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the retrieved serializable object if found; otherwise, null\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject,TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject,TUniqueReference\>\(IEnumerable\<TUniqueReference\>\) Method

Asynchronously retrieves a list of serializable objects associated with the provided unique references\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<USerializableObject>?> GetSerializableObjectsAsync<USerializableObject,TUniqueReference>(System.Collections.Generic.IEnumerable<TUniqueReference>? uniqueReferences)
    where USerializableObject : TSerializableObject
    where TUniqueReference : DiGi.Core.Interfaces.IUniqueReference;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject,TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).USerializableObject'></a>

`USerializableObject`

The type of serializable object to retrieve, which must derive from [TSerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject,TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).TUniqueReference'></a>

`TUniqueReference`

The type of unique reference used for identification, which must implement [DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject,TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).uniqueReferences'></a>

`uniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject,TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject,TUniqueReference\>\(System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of unique references to be used as keys for retrieving the objects\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[USerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject,TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).USerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject,TUniqueReference\>\(System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of retrieved serializable objects, or null if the input references were null or no objects could be found\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(bool)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject\>\(bool\) Method

Asynchronously retrieves a list of serializable objects from the data store\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<USerializableObject>?> GetSerializableObjectsAsync<USerializableObject>(bool inheritance=true)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(bool).USerializableObject'></a>

`USerializableObject`

The type of serializable object to retrieve, which must derive from [TSerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(bool).inheritance'></a>

`inheritance` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to include types derived from [USerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(bool).USerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject\>\(bool\)\.USerializableObject') in the results\. Defaults to `true`\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[USerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(bool).USerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject\>\(bool\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of objects of type [USerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(bool).USerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject\>\(bool\)\.USerializableObject'), or `null` if no objects are found\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(System.Type,bool)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\(Type, bool\) Method

Asynchronously removes the specified type from the collection\.

```csharp
public System.Threading.Tasks.Task<bool> RemoveAsync(System.Type? type, bool inheritance=true);
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(System.Type,bool).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type') to remove\. Can be null\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(System.Type,bool).inheritance'></a>

`inheritance` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether types that inherit from the specified type should also be removed\. Defaults to `true`\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains `true` if the type was successfully removed; otherwise, `false`\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\<TUniqueReference\>\(IEnumerable\<TUniqueReference\>\) Method

Asynchronously removes the entities associated with the specified collection of unique references\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<TUniqueReference>?> RemoveAsync<TUniqueReference>(System.Collections.Generic.IEnumerable<TUniqueReference>? uniqueReferences)
    where TUniqueReference : DiGi.Core.Interfaces.IUniqueReference;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).TUniqueReference'></a>

`TUniqueReference`

The type of the unique reference, which must implement [DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).uniqueReferences'></a>

`uniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\<TUniqueReference\>\(System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of unique references to be removed\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_TUniqueReference_).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\<TUniqueReference\>\(System\.Collections\.Generic\.IEnumerable\<TUniqueReference\>\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of the unique references that were successfully removed, or [null](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/null 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/keywords/null') if the input was null or no items were removed\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_TUniqueReference_(TUniqueReference)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\<TUniqueReference\>\(TUniqueReference\) Method

Asynchronously removes an item identified by the specified unique reference\.

```csharp
public System.Threading.Tasks.Task<TUniqueReference?> RemoveAsync<TUniqueReference>(TUniqueReference? uniqueReference)
    where TUniqueReference : DiGi.Core.Interfaces.IUniqueReference;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_TUniqueReference_(TUniqueReference).TUniqueReference'></a>

`TUniqueReference`

The type of the unique reference, which must implement [DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_TUniqueReference_(TUniqueReference).uniqueReference'></a>

`uniqueReference` [TUniqueReference](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_TUniqueReference_(TUniqueReference).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\<TUniqueReference\>\(TUniqueReference\)\.TUniqueReference')

The unique reference of the item to be removed\. This value can be null\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[TUniqueReference](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_TUniqueReference_(TUniqueReference).TUniqueReference 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\<TUniqueReference\>\(TUniqueReference\)\.TUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the removed unique reference if the operation was successful; otherwise, null\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_USerializableObject_(bool)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\<USerializableObject\>\(bool\) Method

Asynchronously removes an object of the specified serializable type from the data store\.

```csharp
public System.Threading.Tasks.Task<bool> RemoveAsync<USerializableObject>(bool inheritance=true)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_USerializableObject_(bool).USerializableObject'></a>

`USerializableObject`

The type of the serializable object to remove, which must derive from [TSerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync_USerializableObject_(bool).inheritance'></a>

`inheritance` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether the removal process should include inherited types\. Defaults to [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool') if the object was successfully removed; otherwise, [false](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync(TSerializableObject)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.UpdateAsync\(TSerializableObject\) Method

Asynchronously updates the specified serializable object in the data store\.

```csharp
public System.Threading.Tasks.Task<DiGi.Core.Classes.UniqueReference?> UpdateAsync(TSerializableObject? serializableObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync(TSerializableObject).serializableObject'></a>

`serializableObject` [TSerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')

The serializable object instance to update\. Can be null\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[DiGi\.Core\.Classes\.UniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.uniquereference 'DiGi\.Core\.Classes\.UniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the [DiGi\.Core\.Classes\.UniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.uniquereference 'DiGi\.Core\.Classes\.UniqueReference') of the updated object, or null if the update could not be completed or the input was null\.

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_)'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.UpdateAsync\<USerializableObject\>\(IEnumerable\<USerializableObject\>\) Method

Asynchronously updates a collection of serializable objects and returns the set of unique references associated with the updated entities\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.Core.Classes.UniqueReference>?> UpdateAsync<USerializableObject>(System.Collections.Generic.IEnumerable<USerializableObject>? serializableObjects)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_).USerializableObject'></a>

`USerializableObject`

The type of the serializable object to update, which must implement or derive from [TSerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')\.
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_).serializableObjects'></a>

`serializableObjects` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[USerializableObject](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_).USerializableObject 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.UpdateAsync\<USerializableObject\>\(System\.Collections\.Generic\.IEnumerable\<USerializableObject\>\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of objects to be processed for updates\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[DiGi\.Core\.Classes\.UniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.uniquereference 'DiGi\.Core\.Classes\.UniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a HashSet of UniqueReference of unique references if the update was performed; otherwise, `null`\.
### Events

<a name='DiGi.PostgreSQL.UniqueReference.Classes.UniqueReferencePostgreSQLConverter_TSerializableObject_.UniqueIdReferenceGenerating'></a>

## UniqueReferencePostgreSQLConverter\<TSerializableObject\>\.UniqueIdReferenceGenerating Event

Occurs when a unique identifier reference is being generated\.

```csharp
public event UniqueIdReferenceGeneratingEventHandler? UniqueIdReferenceGenerating;
```

#### Event Type
[UniqueIdReferenceGeneratingEventHandler\(object, UniqueIdReferenceGeneratingEventArgs\)](DiGi.PostgreSQL.UniqueReference.Delegates.md#DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs) 'DiGi\.PostgreSQL\.UniqueReference\.Delegates\.UniqueIdReferenceGeneratingEventHandler\(object, DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueIdReferenceGeneratingEventArgs\)')