#### [DiGi\.PostgreSQL\.PartitionReference](index.md 'index')

## DiGi\.PostgreSQL\.PartitionReference\.Classes Namespace
### Classes

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference'></a>

## PartitionReference Class

Represents a reference to a partition\.

```csharp
public class PartitionReference : DiGi.Core.Classes.SerializableReference
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → [DiGi\.Core\.Classes\.SerializableReference](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializablereference 'DiGi\.Core\.Classes\.SerializableReference') → PartitionReference
### Constructors

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.PartitionReference()'></a>

## PartitionReference\(\) Constructor

Initializes a new instance of the [PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference') class\.

```csharp
public PartitionReference();
```

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.PartitionReference(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference)'></a>

## PartitionReference\(PartitionReference\) Constructor

Initializes a new instance of the [PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference') class by copying another reference\.

```csharp
public PartitionReference(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference? partitionReference);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.PartitionReference(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference).partitionReference'></a>

`partitionReference` [PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')

The source reference to copy\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.PartitionReference(string,string)'></a>

## PartitionReference\(string, string\) Constructor

Initializes a new instance of the [PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference') class with a specified name and unique identifier\.

```csharp
public PartitionReference(string? name, string? uniqueId);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.PartitionReference(string,string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.PartitionReference(string,string).uniqueId'></a>

`uniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the partition\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.PartitionReference(System.Text.Json.Nodes.JsonObject)'></a>

## PartitionReference\(JsonObject\) Constructor

Initializes a new instance of the [PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference') class from a JSON object\.

```csharp
public PartitionReference(System.Text.Json.Nodes.JsonObject? jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.PartitionReference(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The JSON object to initialize from\.
### Properties

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.Name'></a>

## PartitionReference\.Name Property

Gets the name of the partition reference\.

```csharp
public string? Name { get; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.UniqueId'></a>

## PartitionReference\.UniqueId Property

Gets the unique identifier for this reference\.

```csharp
public string? UniqueId { get; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')
### Methods

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.Clone()'></a>

## PartitionReference\.Clone\(\) Method

Creates a deep copy of the current partition reference\.

```csharp
public override DiGi.Core.Interfaces.ISerializableObject? Clone();
```

Implements [Clone\(\)](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1.clone 'DiGi\.Core\.Interfaces\.ICloneableObject\`1\.Clone')

#### Returns
[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')  
A new [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject') instance that is a clone of the current object\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.Equals(object)'></a>

## PartitionReference\.Equals\(object\) Method

Determines whether the specified object is equal to the current partition reference\.

```csharp
public override bool Equals(object? obj);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.Equals(object).obj'></a>

`obj` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The object to compare with the current object\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the objects are equal; otherwise, false\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.GetHashCode()'></a>

## PartitionReference\.GetHashCode\(\) Method

Returns the hash code for the current partition reference\.

```csharp
public override int GetHashCode();
```

#### Returns
[System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')  
A 32\-bit signed integer hash code\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference.ToString()'></a>

## PartitionReference\.ToString\(\) Method

Returns a string representation of the partition reference, combining the name and unique identifier with a separator\.

```csharp
public override string? ToString();
```

#### Returns
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')  
A string representing the partition reference, or null if the name or unique identifier is empty\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs'></a>

## PartitionReferenceGeneratingEventArgs Class

Provides data for events that occur during the generation of a partition reference\.

```csharp
public class PartitionReferenceGeneratingEventArgs : DiGi.PostgreSQL.Classes.ReferenceGeneratingEventArgs
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [System\.EventArgs](https://learn.microsoft.com/en-us/dotnet/api/system.eventargs 'System\.EventArgs') → [DiGi\.PostgreSQL\.Classes\.ReferenceGeneratingEventArgs](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.referencegeneratingeventargs 'DiGi\.PostgreSQL\.Classes\.ReferenceGeneratingEventArgs') → PartitionReferenceGeneratingEventArgs
### Constructors

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs.PartitionReferenceGeneratingEventArgs(object)'></a>

## PartitionReferenceGeneratingEventArgs\(object\) Constructor

Initializes a new instance of the [PartitionReferenceGeneratingEventArgs](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferenceGeneratingEventArgs') class\.

```csharp
public PartitionReferenceGeneratingEventArgs(object? item);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs.PartitionReferenceGeneratingEventArgs(object).item'></a>

`item` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The item associated with the event\.
### Properties

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs.PartitionReference'></a>

## PartitionReferenceGeneratingEventArgs\.PartitionReference Property

Gets or sets the partition reference associated with this event\.

```csharp
public DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference? PartitionReference { get; set; }
```

#### Property Value
[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter'></a>

## PartitionReferencePostgreSQLConverter Class

A non\-generic implementation of the partition reference PostgreSQL converter using ISerializableObject\.

```csharp
public class PartitionReferencePostgreSQLConverter : DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter<DiGi.Core.Interfaces.ISerializableObject>
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1') → [DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter&lt;](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_ 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter\<TSerializableObject\>')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_ 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter\<TSerializableObject\>') → PartitionReferencePostgreSQLConverter
### Constructors

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter.PartitionReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## PartitionReferencePostgreSQLConverter\(ConnectionData\) Constructor

Initializes a new instance of the [PartitionReferencePostgreSQLConverter](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter') class\.

```csharp
public PartitionReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter.PartitionReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to establish a database connection\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\> Class

Provides a converter for managing partition references within a PostgreSQL database for serializable objects\.

```csharp
public class PartitionReferencePostgreSQLConverter<TSerializableObject> : DiGi.PostgreSQL.Classes.PostgreSQLConverter<TSerializableObject>
    where TSerializableObject : DiGi.Core.Interfaces.ISerializableObject
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject'></a>

`TSerializableObject`

The type of the serializable object that implements ISerializableObject\.

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1')[TSerializableObject](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1') → PartitionReferencePostgreSQLConverter\<TSerializableObject\>

Derived  
↳ [PartitionReferencePostgreSQLConverter](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter')
### Constructors

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.PartitionReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## PartitionReferencePostgreSQLConverter\(ConnectionData\) Constructor

Initializes a new instance of the [PartitionReferencePostgreSQLConverter&lt;TSerializableObject&gt;](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_ 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter\<TSerializableObject\>') class\.

```csharp
public PartitionReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.PartitionReferencePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data used to establish a database connection\.
### Methods

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.ContainsAsync\<TUniqueReference\>\(IEnumerable\<PartitionReference\>\) Method

Asynchronously checks if the specified partition references exist in the database\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference>?> ContainsAsync<TUniqueReference>(System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference>? partitionReferences);
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_).TUniqueReference'></a>

`TUniqueReference`

The type of the unique reference used for the check\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.ContainsAsync_TUniqueReference_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_).partitionReferences'></a>

`partitionReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of partition references to verify\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A hash set containing the existing partition references, or null if the input is null or connection fails\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.CountAsync(string)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.CountAsync\(string\) Method

Asynchronously counts the number of elements associated with a specific partition name\.

```csharp
public System.Threading.Tasks.Task<long> CountAsync(string name);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.CountAsync(string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition to count\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Int64](https://learn.microsoft.com/en-us/dotnet/api/system.int64 'System\.Int64')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
The total count of elements, or \-1 if the name is null or an error occurs\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetDataType(string)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.GetDataType\(string\) Method

Retrieves the data type associated with a given partition name\.

```csharp
public virtual DiGi.PostgreSQL.Enums.DataType GetDataType(string? name);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetDataType(string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition\.

#### Returns
[DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType')  
The [DiGi\.PostgreSQL\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.enums.datatype 'DiGi\.PostgreSQL\.Enums\.DataType') of the partition, or DataType\.Undefined if the name is null or whitespace\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectAsync\<USerializableObject\>\(PartitionReference\) Method

Asynchronously retrieves a single serializable object associated with the specified partition reference\.

```csharp
public System.Threading.Tasks.Task<USerializableObject?> GetSerializableObjectAsync<USerializableObject>(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference? partitionReference)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference).USerializableObject'></a>

`USerializableObject`

The specific type of the serializable object\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference).partitionReference'></a>

`partitionReference` [PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')

The partition reference to retrieve the object for\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[USerializableObject](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectAsync_USerializableObject_(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference).USerializableObject 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectAsync\<USerializableObject\>\(DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
The retrieved serializable object, or default if not found or input is null\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(string)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject\>\(string\) Method

Asynchronously retrieves a list of serializable objects associated with the specified partition name\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<USerializableObject>?> GetSerializableObjectsAsync<USerializableObject>(string? name)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(string).USerializableObject'></a>

`USerializableObject`

The specific type of the serializable object\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[USerializableObject](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(string).USerializableObject 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject\>\(string\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A list of retrieved serializable objects, or default if input is null or connection fails\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject\>\(IEnumerable\<PartitionReference\>\) Method

Asynchronously retrieves a list of serializable objects associated with the specified partition references\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<USerializableObject>?> GetSerializableObjectsAsync<USerializableObject>(System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference>? partitionReferences)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_).USerializableObject'></a>

`USerializableObject`

The specific type of the serializable object\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_).partitionReferences'></a>

`partitionReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of partition references to retrieve objects for\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[USerializableObject](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.GetSerializableObjectsAsync_USerializableObject_(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_).USerializableObject 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.GetSerializableObjectsAsync\<USerializableObject\>\(System\.Collections\.Generic\.IEnumerable\<DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference\>\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A list of retrieved serializable objects, or null if input is null or connection fails\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\(PartitionReference\) Method

Asynchronously removes a single partition reference from the database\.

```csharp
public System.Threading.Tasks.Task<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference?> RemoveAsync(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference? partitionReference);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference).partitionReference'></a>

`partitionReference` [PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')

The partition reference to remove\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
The removed partition reference, or default if not found or input is null\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(string)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\(string\) Method

Asynchronously removes a partition reference identified by its name\.

```csharp
public System.Threading.Tasks.Task<bool> RemoveAsync(string? name);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(string).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition to remove\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
True if the partition was successfully removed, otherwise false\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\(IEnumerable\<PartitionReference\>\) Method

Asynchronously removes a collection of partition references from the database\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference>?> RemoveAsync(System.Collections.Generic.IEnumerable<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference>? partitionReferences);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(System.Collections.Generic.IEnumerable_DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference_).partitionReferences'></a>

`partitionReferences` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of partition references to remove\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A hash set containing the removed partition references, or null if input is null or connection fails\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(System.Collections.Generic.IEnumerable_string_)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.RemoveAsync\(IEnumerable\<string\>\) Method

Asynchronously removes multiple partition references identified by their names\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<string>?> RemoveAsync(System.Collections.Generic.IEnumerable<string>? names);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.RemoveAsync(System.Collections.Generic.IEnumerable_string_).names'></a>

`names` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of partition names to remove\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A hash set containing the names of successfully removed partitions, or null if input is null\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync(TSerializableObject)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.UpdateAsync\(TSerializableObject\) Method

Asynchronously updates a single serializable object in the database\.

```csharp
public System.Threading.Tasks.Task<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference?> UpdateAsync(TSerializableObject? serializableObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync(TSerializableObject).serializableObject'></a>

`serializableObject` [TSerializableObject](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.TSerializableObject 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.TSerializableObject')

The serializable object to update\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
The updated partition reference, or null if input is null or update fails\.

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_)'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.UpdateAsync\<USerializableObject\>\(IEnumerable\<USerializableObject\>\) Method

Asynchronously updates a collection of serializable objects in the database\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference>?> UpdateAsync<USerializableObject>(System.Collections.Generic.IEnumerable<USerializableObject>? serializableObjects)
    where USerializableObject : TSerializableObject;
```
#### Type parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_).USerializableObject'></a>

`USerializableObject`

The specific type of the serializable object\.
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_).serializableObjects'></a>

`serializableObjects` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[USerializableObject](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.UpdateAsync_USerializableObject_(System.Collections.Generic.IEnumerable_USerializableObject_).USerializableObject 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.UpdateAsync\<USerializableObject\>\(System\.Collections\.Generic\.IEnumerable\<USerializableObject\>\)\.USerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of serializable objects to update\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A hash set containing the updated partition references, or null if input is null or connection fails\.
### Events

<a name='DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferencePostgreSQLConverter_TSerializableObject_.PartitionReferenceGenerating'></a>

## PartitionReferencePostgreSQLConverter\<TSerializableObject\>\.PartitionReferenceGenerating Event

Event that is triggered when a partition reference is being generated\.

```csharp
public event PartitionReferenceGeneratingEventHandler? PartitionReferenceGenerating;
```

#### Event Type
[PartitionReferenceGeneratingEventHandler\(object, PartitionReferenceGeneratingEventArgs\)](DiGi.PostgreSQL.PartitionReference.Delegates.md#DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs) 'DiGi\.PostgreSQL\.PartitionReference\.Delegates\.PartitionReferenceGeneratingEventHandler\(object, DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferenceGeneratingEventArgs\)')