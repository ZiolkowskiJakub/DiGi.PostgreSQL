#### [DiGi\.PostgreSQL\.PartitionUniqueReference](index.md 'index')

## DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes Namespace
### Classes

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference'></a>

## PartitionUniqueReference Class

Represents a unique reference for a partition\.

```csharp
public class PartitionUniqueReference : DiGi.Core.Classes.SerializableReference
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → [DiGi\.Core\.Classes\.SerializableReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializablereference 'DiGi\.Core\.Classes\.SerializableReference') → PartitionUniqueReference
### Constructors

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.PartitionUniqueReference()'></a>

## PartitionUniqueReference\(\) Constructor

Initializes a new instance of the [PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference') class\.

```csharp
public PartitionUniqueReference();
```

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.PartitionUniqueReference(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference)'></a>

## PartitionUniqueReference\(PartitionUniqueReference\) Constructor

Initializes a new instance of the [PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference') class by copying an existing instance\.

```csharp
public PartitionUniqueReference(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference? partitionUniqueReference);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.PartitionUniqueReference(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference).partitionUniqueReference'></a>

`partitionUniqueReference` [PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')

The source instance to copy\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.PartitionUniqueReference(string,DiGi.Core.Interfaces.IUniqueReference)'></a>

## PartitionUniqueReference\(string, IUniqueReference\) Constructor

Initializes a new instance of the [PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference') class with the specified name and unique reference\.

```csharp
public PartitionUniqueReference(string? name, DiGi.Core.Interfaces.IUniqueReference? uniqueReference);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.PartitionUniqueReference(string,DiGi.Core.Interfaces.IUniqueReference).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.PartitionUniqueReference(string,DiGi.Core.Interfaces.IUniqueReference).uniqueReference'></a>

`uniqueReference` [DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference')

The unique reference for the partition\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.PartitionUniqueReference(System.Text.Json.Nodes.JsonObject)'></a>

## PartitionUniqueReference\(JsonObject\) Constructor

Initializes a new instance of the [PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference') class from a JSON object\.

```csharp
public PartitionUniqueReference(System.Text.Json.Nodes.JsonObject? jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.PartitionUniqueReference(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The JSON object to initialize from\.
### Properties

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.Name'></a>

## PartitionUniqueReference\.Name Property

Gets the name of the partition unique reference\.

```csharp
public string? Name { get; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.UniqueReference'></a>

## PartitionUniqueReference\.UniqueReference Property

Gets the unique reference associated with the partition\.

```csharp
public DiGi.Core.Interfaces.IUniqueReference? UniqueReference { get; }
```

#### Property Value
[DiGi\.Core\.Interfaces\.IUniqueReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iuniquereference 'DiGi\.Core\.Interfaces\.IUniqueReference')
### Methods

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.Clone()'></a>

## PartitionUniqueReference\.Clone\(\) Method

Creates a deep copy of the current partition unique reference\.

```csharp
public override DiGi.Core.Interfaces.ISerializableObject? Clone();
```

Implements [Clone\(\)](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1.clone 'DiGi\.Core\.Interfaces\.ICloneableObject\`1\.Clone')

#### Returns
[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')  
A new [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject') instance that is a clone of this object\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.Equals(object)'></a>

## PartitionUniqueReference\.Equals\(object\) Method

Determines whether the specified object is equal to the current partition unique reference\.

```csharp
public override bool Equals(object? obj);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.Equals(object).obj'></a>

`obj` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The object to compare with the current instance\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the objects are equal; otherwise, false\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.GetHashCode()'></a>

## PartitionUniqueReference\.GetHashCode\(\) Method

Gets the hash code for the current partition unique reference\.

```csharp
public override int GetHashCode();
```

#### Returns
[System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')  
A 32\-bit signed integer hash code\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference.ToString()'></a>

## PartitionUniqueReference\.ToString\(\) Method

Returns a string representation of the partition unique reference\.

```csharp
public override string? ToString();
```

#### Returns
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')  
A string combining the name and unique reference, or null if either is missing\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferenceGeneratingEventArgs'></a>

## PartitionUniqueReferenceGeneratingEventArgs Class

Provides data for events that occur during the generation of a partition unique reference\.

```csharp
public class PartitionUniqueReferenceGeneratingEventArgs : DiGi.PostgreSQL.Classes.ReferenceGeneratingEventArgs
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [System\.EventArgs](https://learn.microsoft.com/en-us/dotnet/api/system.eventargs 'System\.EventArgs') → [DiGi\.PostgreSQL\.Classes\.ReferenceGeneratingEventArgs](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.referencegeneratingeventargs 'DiGi\.PostgreSQL\.Classes\.ReferenceGeneratingEventArgs') → PartitionUniqueReferenceGeneratingEventArgs
### Constructors

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferenceGeneratingEventArgs.PartitionUniqueReferenceGeneratingEventArgs(object)'></a>

## PartitionUniqueReferenceGeneratingEventArgs\(object\) Constructor

Initializes a new instance of the [PartitionUniqueReferenceGeneratingEventArgs](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferenceGeneratingEventArgs 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferenceGeneratingEventArgs') class\.

```csharp
public PartitionUniqueReferenceGeneratingEventArgs(object? item);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferenceGeneratingEventArgs.PartitionUniqueReferenceGeneratingEventArgs(object).item'></a>

`item` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The item for which the reference is being generated\.
### Properties

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferenceGeneratingEventArgs.PartitionReference'></a>

## PartitionUniqueReferenceGeneratingEventArgs\.PartitionReference Property

Gets or sets the partition reference associated with this event\.

```csharp
public DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference? PartitionReference { get; set; }
```

#### Property Value
[DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.partitionreference.classes.partitionreference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferenceGeneratingEventArgs.PartitionUniqueReference'></a>

## PartitionUniqueReferenceGeneratingEventArgs\.PartitionUniqueReference Property

Gets the generated partition unique reference based on the provided item and partition reference\.

```csharp
public DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference? PartitionUniqueReference { get; }
```

#### Property Value
[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter'></a>

## PartitionUniqueReferencePostgreSQLConverter Class

A non\-generic implementation of the partition unique reference converter using [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')\.

```csharp
public class PartitionUniqueReferencePostgreSQLConverter : DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter<DiGi.Core.Interfaces.ISerializableObject>
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1') → [DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter&lt;](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_ 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_ 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>') → PartitionUniqueReferencePostgreSQLConverter
### Constructors

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter.PartitionUniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## PartitionUniqueReferencePostgreSQLConverter\(ConnectionData\) Constructor

Initializes a new instance of the [PartitionUniqueReferencePostgreSQLConverter](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter') class\.

```csharp
public PartitionUniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter.PartitionUniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to establish a database connection\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\> Class

Provides functionality to convert and manage partition unique references within a PostgreSQL database for objects implementing [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')\.

```csharp
public class PartitionUniqueReferencePostgreSQLConverter<TSerializableObject> : DiGi.PostgreSQL.Classes.PostgreSQLConverter<TSerializableObject>
    where TSerializableObject : DiGi.Core.Interfaces.ISerializableObject
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject'></a>

`TSerializableObject`

The type of the serializable object, which must implement [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')\.

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1')[TSerializableObject](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1') → PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>

Derived  
↳ [PartitionUniqueReferencePostgreSQLConverter](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter')
### Constructors

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.PartitionUniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## PartitionUniqueReferencePostgreSQLConverter\(ConnectionData\) Constructor

Initializes a new instance of the [PartitionUniqueReferencePostgreSQLConverter&lt;TSerializableObject&gt;](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_ 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>') class\.

```csharp
public PartitionUniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.PartitionUniqueReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to establish a database connection\.
### Methods

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.Clean(bool,bool)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.Clean\(bool, bool\) Method

Cleans the specified partitions and types from the database\.

```csharp
public System.Threading.Tasks.Task<bool> Clean(bool partitions=true, bool types=true);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.Clean(bool,bool).partitions'></a>

`partitions` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to clean partitions\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.Clean(bool,bool).types'></a>

`types` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to clean types\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains true if any partitions or types were cleaned; otherwise, false\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync(DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.ContainsAsync\(Type\) Method

Checks asynchronously whether the specified type exists in the database\.

```csharp
public System.Threading.Tasks.Task<bool> ContainsAsync(DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type? type);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync(DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type).type'></a>

`type` [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type')

The type to check for existence\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains true if the type exists; otherwise, false\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.ContainsAsync\(IEnumerable\<PartitionUniqueReference\>\) Method

Checks asynchronously whether the specified collection of partition unique references exists in the database\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference>?> ContainsAsync(System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference>? partitionUniqueReferences);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_).partitionUniqueReferences'></a>

`partitionUniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of unique references to check\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a hash set of existing unique references, or null if the input was null or connection failed\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync(System.Type)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.ContainsAsync\(Type\) Method

Checks asynchronously whether the specified system type exists in the database\.

```csharp
public System.Threading.Tasks.Task<bool> ContainsAsync(System.Type? type);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync(System.Type).type'></a>

`type` [System\.Type](https://learn.microsoft.com/en-us/dotnet/api/system.type 'System\.Type')

The system type to check for existence\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains true if the type exists; otherwise, false\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.GetDataType(string)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetDataType\(string\) Method

Gets the data type associated with the specified name\.

```csharp
public virtual DiGi.PostgreSQL.Enums.DataType GetDataType(string? name);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.GetDataType(string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the entity to determine the data type for\.

#### Returns
[DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType')  
The [DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType') associated with the name, or [DiGi\.PostgreSQL\.Enums\.DataType\.Undefined](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype.undefined 'DiGi\.PostgreSQL\.Enums\.DataType\.Undefined') if the name is null or whitespace\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectAsync\<USerializableObject\>\(PartitionUniqueReference\) Method

Retrieves a single serializable object asynchronously based on the provided unique reference\.

```csharp
public System.Threading.Tasks.Task<USerializableObject?> GetSerializableObjectAsync<USerializableObject>(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference? partitionUniqueReference)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference).USerializableObject'></a>

`USerializableObject`

The specific type of the serializable object\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference).partitionUniqueReference'></a>

`partitionUniqueReference` [PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')

The unique reference used to locate the object\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[USerializableObject](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference).USerializableObject 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectAsync\<USerializableObject\>\(DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the found object, or default if not found or input is null\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject\>\(IEnumerable\<PartitionUniqueReference\>\) Method

Retrieves a list of serializable objects asynchronously based on the provided collection of unique references\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<USerializableObject>?> GetSerializableObjectsAsync<USerializableObject>(System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference>? partitionUniqueReferences)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_).USerializableObject'></a>

`USerializableObject`

The specific type of the serializable objects\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_).partitionUniqueReferences'></a>

`partitionUniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of unique references used to locate the objects\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[USerializableObject](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_).USerializableObject 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject\>\(System\.Collections\.Generic\.IEnumerable\<DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference\>\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of found objects, or null if input is null or connection failed\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference,bool)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\(PartitionUniqueReference, bool\) Method

Removes a single unique reference from the database asynchronously\.

```csharp
public System.Threading.Tasks.Task<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference?> RemoveAsync(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference? partitionUniqueReference, bool clean=true);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference,bool).partitionUniqueReference'></a>

`partitionUniqueReference` [PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')

The unique reference to remove\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference,bool).clean'></a>

`clean` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to perform a cleaning operation after removal\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the removed unique reference, or null if not found or input is null\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_,bool)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\(IEnumerable\<PartitionUniqueReference\>, bool\) Method

Removes the specified collection of unique references from the database asynchronously\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference>?> RemoveAsync(System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference>? partitionUniqueReferences, bool clean=true);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_,bool).partitionUniqueReferences'></a>

`partitionUniqueReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of unique references to remove\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference_,bool).clean'></a>

`clean` [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

A value indicating whether to perform a cleaning operation after removal\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a hash set of removed unique references, or null if input is null or connection failed\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync(TSerializableObject)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.UpdateAsync\(TSerializableObject\) Method

Updates a single serializable object in the database asynchronously\.

```csharp
public System.Threading.Tasks.Task<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference?> UpdateAsync(TSerializableObject? serializableObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync(TSerializableObject).serializableObject'></a>

`serializableObject` [TSerializableObject](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')

The object to update\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the unique reference of the updated object, or null if input is null or update failed\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_)'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.UpdateAsync\<USerializableObject\>\(IEnumerable\<USerializableObject\>\) Method

Updates a collection of serializable objects in the database asynchronously\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference>?> UpdateAsync<USerializableObject>(System.Collections.Generic.IEnumerable<USerializableObject>? serializableObjects)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_).USerializableObject'></a>

`USerializableObject`

The specific type of the serializable objects\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_).serializableObjects'></a>

`serializableObjects` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[USerializableObject](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_).USerializableObject 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.UpdateAsync\<USerializableObject\>\(System\.Collections\.Generic\.IEnumerable\<USerializableObject\>\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The collection of objects to update\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionUniqueReference](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReference 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a hash set of unique references for the updated objects, or null if input is null or connection failed\.
### Events

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferencePostgreSQLConverter_TSerializableObject_.PartitionUniqueReferenceReferenceGenerating'></a>

## PartitionUniqueReferencePostgreSQLConverter\<TSerializableObject\>\.PartitionUniqueReferenceReferenceGenerating Event

Occurs when a partition unique reference is being generated\.

```csharp
public event PartitionUniqueReferenceGeneratingEventHandler? PartitionUniqueReferenceReferenceGenerating;
```

#### Event Type
[PartitionUniqueReferenceGeneratingEventHandler\(object, PartitionUniqueReferenceGeneratingEventArgs\)](DiGi.PostgreSQL.PartitionUniqueReference.Delegates.md#DiGi.PostgreSQL.PartitionUniqueReference.Delegates.PartitionUniqueReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.PartitionUniqueReference.Classes.PartitionUniqueReferenceGeneratingEventArgs) 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Delegates\.PartitionUniqueReferenceGeneratingEventHandler\(object, DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.PartitionUniqueReferenceGeneratingEventArgs\)')

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type'></a>

## Type Class

Represents a type definition with an ID and name, used for partitioning unique references\.

```csharp
public class Type : DiGi.Core.Classes.SerializableObject, DiGi.PostgreSQL.Interfaces.IPostgreSQLSerializableObject, DiGi.PostgreSQL.Interfaces.IPostgreSQLObject, DiGi.Core.Interfaces.IObject, DiGi.Core.Interfaces.ISerializableObject, DiGi.Core.Interfaces.ICloneableObject<DiGi.Core.Interfaces.ISerializableObject>, DiGi.Core.Interfaces.ICloneableObject
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → Type

Implements [DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLSerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.interfaces.ipostgresqlserializableobject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLSerializableObject'), [DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLObject](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.interfaces.ipostgresqlobject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLObject'), [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject'), [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject'), [DiGi\.Core\.Interfaces\.ICloneableObject&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1 'DiGi\.Core\.Interfaces\.ICloneableObject\`1')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1 'DiGi\.Core\.Interfaces\.ICloneableObject\`1'), [DiGi\.Core\.Interfaces\.ICloneableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject 'DiGi\.Core\.Interfaces\.ICloneableObject')
### Constructors

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type.Type(DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type)'></a>

## Type\(Type\) Constructor

Initializes a new instance of the [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type') class by copying another instance\.

```csharp
public Type(DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type type);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type.Type(DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type).type'></a>

`type` [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type')

The type instance to copy\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type.Type(short,string)'></a>

## Type\(short, string\) Constructor

Initializes a new instance of the [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type') class with the specified ID and name\.

```csharp
public Type(short id, string name);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type.Type(short,string).id'></a>

`id` [System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')

The unique identifier of the type\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type.Type(short,string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the type\.

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type.Type(System.Text.Json.Nodes.JsonObject)'></a>

## Type\(JsonObject\) Constructor

Initializes a new instance of the [Type](DiGi.PostgreSQL.PartitionUniqueReference.Classes.md#DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type 'DiGi\.PostgreSQL\.PartitionUniqueReference\.Classes\.Type') class from a JSON object\.

```csharp
public Type(System.Text.Json.Nodes.JsonObject jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type.Type(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The JSON object containing the type definition data\.
### Properties

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type.Id'></a>

## Type\.Id Property

Gets the unique identifier of the type\.

```csharp
public short Id { get; }
```

#### Property Value
[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')

<a name='DiGi.PostgreSQL.PartitionUniqueReference.Classes.Type.Name'></a>

## Type\.Name Property

Gets the name of the type\.

```csharp
public string? Name { get; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')