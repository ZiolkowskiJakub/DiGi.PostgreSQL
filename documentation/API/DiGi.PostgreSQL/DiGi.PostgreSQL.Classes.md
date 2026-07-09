#### [DiGi\.PostgreSQL](DiGi.PostgreSQL.Overview.md 'DiGi\.PostgreSQL\.Overview')

## DiGi\.PostgreSQL\.Classes Namespace
### Classes

<a name='DiGi.PostgreSQL.Classes.ConnectionData'></a>

## ConnectionData Class

Represents the connection settings required to establish a connection to a PostgreSQL database\.

```csharp
public class ConnectionData : DiGi.Core.Classes.SerializableObject, DiGi.PostgreSQL.Interfaces.IPostgreSQLSerializableObject, DiGi.PostgreSQL.Interfaces.IPostgreSQLObject, DiGi.Core.Interfaces.IObject, DiGi.Core.Interfaces.ISerializableObject, DiGi.Core.Interfaces.ICloneableObject<DiGi.Core.Interfaces.ISerializableObject>, DiGi.Core.Interfaces.ICloneableObject
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → ConnectionData

Implements [IPostgreSQLSerializableObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLSerializableObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLSerializableObject'), [IPostgreSQLObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLObject'), [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject'), [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject'), [DiGi\.Core\.Interfaces\.ICloneableObject&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1 'DiGi\.Core\.Interfaces\.ICloneableObject\`1')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1 'DiGi\.Core\.Interfaces\.ICloneableObject\`1'), [DiGi\.Core\.Interfaces\.ICloneableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject 'DiGi\.Core\.Interfaces\.ICloneableObject')
### Constructors

<a name='DiGi.PostgreSQL.Classes.ConnectionData.ConnectionData(DiGi.PostgreSQL.Classes.ConnectionData,string)'></a>

## ConnectionData\(ConnectionData, string\) Constructor

Initializes a new instance of the [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData') class based on an existing connection configuration but with a different database\.

```csharp
public ConnectionData(DiGi.PostgreSQL.Classes.ConnectionData connectionData, string database);
```
#### Parameters

<a name='DiGi.PostgreSQL.Classes.ConnectionData.ConnectionData(DiGi.PostgreSQL.Classes.ConnectionData,string).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The source connection data containing host, username, password, and port\.

<a name='DiGi.PostgreSQL.Classes.ConnectionData.ConnectionData(DiGi.PostgreSQL.Classes.ConnectionData,string).database'></a>

`database` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the database to connect to\.

<a name='DiGi.PostgreSQL.Classes.ConnectionData.ConnectionData(string,string,string,string,System.Nullable_int_)'></a>

## ConnectionData\(string, string, string, string, Nullable\<int\>\) Constructor

Initializes a new instance of the [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData') class with specified connection details\.

```csharp
public ConnectionData(string? host, string? username, string? password, string? database, System.Nullable<int> port);
```
#### Parameters

<a name='DiGi.PostgreSQL.Classes.ConnectionData.ConnectionData(string,string,string,string,System.Nullable_int_).host'></a>

`host` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The server host address\.

<a name='DiGi.PostgreSQL.Classes.ConnectionData.ConnectionData(string,string,string,string,System.Nullable_int_).username'></a>

`username` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The user name for authentication\.

<a name='DiGi.PostgreSQL.Classes.ConnectionData.ConnectionData(string,string,string,string,System.Nullable_int_).password'></a>

`password` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The password for authentication\.

<a name='DiGi.PostgreSQL.Classes.ConnectionData.ConnectionData(string,string,string,string,System.Nullable_int_).database'></a>

`database` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the database to connect to\.

<a name='DiGi.PostgreSQL.Classes.ConnectionData.ConnectionData(string,string,string,string,System.Nullable_int_).port'></a>

`port` [System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')

The port number of the PostgreSQL server\.
### Properties

<a name='DiGi.PostgreSQL.Classes.ConnectionData.Database'></a>

## ConnectionData\.Database Property

Gets or sets the name of the PostgreSQL database\.

```csharp
public string? Database { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Classes.ConnectionData.Host'></a>

## ConnectionData\.Host Property

Gets or sets the server host address\.

```csharp
public string? Host { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Classes.ConnectionData.Password'></a>

## ConnectionData\.Password Property

Gets or sets the password for authentication\.

```csharp
public string? Password { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Classes.ConnectionData.Port'></a>

## ConnectionData\.Port Property

Gets or sets the port number of the PostgreSQL server\. Defaults to 5432\.

```csharp
public System.Nullable<int> Port { get; set; }
```

#### Property Value
[System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')

<a name='DiGi.PostgreSQL.Classes.ConnectionData.Username'></a>

## ConnectionData\.Username Property

Gets or sets the user name for authentication\.

```csharp
public string? Username { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')
### Methods

<a name='DiGi.PostgreSQL.Classes.ConnectionData.GetDefault()'></a>

## ConnectionData\.GetDefault\(\) Method

Creates a new [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData') instance with default settings, using the current host and credentials but resetting the database and port\.

```csharp
public DiGi.PostgreSQL.Classes.ConnectionData GetDefault();
```

#### Returns
[ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')  
A [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData') object initialized with default values\.

<a name='DiGi.PostgreSQL.Classes.ConnectionData.ToString()'></a>

## ConnectionData\.ToString\(\) Method

Returns a string representation of the connection data in a semicolon\-separated format\.

```csharp
public override string ToString();
```

#### Returns
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')  
A formatted string containing the connection details\.

<a name='DiGi.PostgreSQL.Classes.Partition'></a>

## Partition Class

Represents a partition within the PostgreSQL database context\.

```csharp
public class Partition : DiGi.Core.Classes.SerializableObject, DiGi.PostgreSQL.Interfaces.IPostgreSQLSerializableObject, DiGi.PostgreSQL.Interfaces.IPostgreSQLObject, DiGi.Core.Interfaces.IObject, DiGi.Core.Interfaces.ISerializableObject, DiGi.Core.Interfaces.ICloneableObject<DiGi.Core.Interfaces.ISerializableObject>, DiGi.Core.Interfaces.ICloneableObject
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → Partition

Implements [IPostgreSQLSerializableObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLSerializableObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLSerializableObject'), [IPostgreSQLObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLObject'), [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject'), [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject'), [DiGi\.Core\.Interfaces\.ICloneableObject&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1 'DiGi\.Core\.Interfaces\.ICloneableObject\`1')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1 'DiGi\.Core\.Interfaces\.ICloneableObject\`1'), [DiGi\.Core\.Interfaces\.ICloneableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject 'DiGi\.Core\.Interfaces\.ICloneableObject')
### Constructors

<a name='DiGi.PostgreSQL.Classes.Partition.Partition(DiGi.PostgreSQL.Classes.Partition)'></a>

## Partition\(Partition\) Constructor

Initializes a new instance of the [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition') class by copying an existing partition\.

```csharp
public Partition(DiGi.PostgreSQL.Classes.Partition partition);
```
#### Parameters

<a name='DiGi.PostgreSQL.Classes.Partition.Partition(DiGi.PostgreSQL.Classes.Partition).partition'></a>

`partition` [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')

The source partition to copy from\.

<a name='DiGi.PostgreSQL.Classes.Partition.Partition(short,string,DiGi.PostgreSQL.Enums.DataType)'></a>

## Partition\(short, string, DataType\) Constructor

Initializes a new instance of the [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition') class with specified values\.

```csharp
public Partition(short id, string name, DiGi.PostgreSQL.Enums.DataType dataType);
```
#### Parameters

<a name='DiGi.PostgreSQL.Classes.Partition.Partition(short,string,DiGi.PostgreSQL.Enums.DataType).id'></a>

`id` [System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')

The unique identifier for the partition\.

<a name='DiGi.PostgreSQL.Classes.Partition.Partition(short,string,DiGi.PostgreSQL.Enums.DataType).name'></a>

`name` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the partition\.

<a name='DiGi.PostgreSQL.Classes.Partition.Partition(short,string,DiGi.PostgreSQL.Enums.DataType).dataType'></a>

`dataType` [DataType](DiGi.PostgreSQL.Enums.md#DiGi.PostgreSQL.Enums.DataType 'DiGi\.PostgreSQL\.Enums\.DataType')

The data type associated with the partition\.

<a name='DiGi.PostgreSQL.Classes.Partition.Partition(System.Text.Json.Nodes.JsonObject)'></a>

## Partition\(JsonObject\) Constructor

Initializes a new instance of the [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition') class using a JSON object\.

```csharp
public Partition(System.Text.Json.Nodes.JsonObject jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.Classes.Partition.Partition(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The JSON object used to initialize the partition\.
### Properties

<a name='DiGi.PostgreSQL.Classes.Partition.DataType'></a>

## Partition\.DataType Property

Gets the data type of the partition\.

```csharp
public DiGi.PostgreSQL.Enums.DataType DataType { get; }
```

#### Property Value
[DataType](DiGi.PostgreSQL.Enums.md#DiGi.PostgreSQL.Enums.DataType 'DiGi\.PostgreSQL\.Enums\.DataType')

<a name='DiGi.PostgreSQL.Classes.Partition.Id'></a>

## Partition\.Id Property

Gets the unique identifier of the partition\.

```csharp
public short Id { get; }
```

#### Property Value
[System\.Int16](https://learn.microsoft.com/en-us/dotnet/api/system.int16 'System\.Int16')

<a name='DiGi.PostgreSQL.Classes.Partition.Name'></a>

## Partition\.Name Property

Gets the name of the partition\.

```csharp
public string? Name { get; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile'></a>

## PostgreSQLConfigurationFile Class

Represents a configuration file specifically for PostgreSQL database settings\.

```csharp
public class PostgreSQLConfigurationFile : DiGi.Core.Classes.ConfigurationFile, DiGi.PostgreSQL.Interfaces.IPostgreSQLSerializableObject, DiGi.PostgreSQL.Interfaces.IPostgreSQLObject, DiGi.Core.Interfaces.IObject, DiGi.Core.Interfaces.ISerializableObject, DiGi.Core.Interfaces.ICloneableObject<DiGi.Core.Interfaces.ISerializableObject>, DiGi.Core.Interfaces.ICloneableObject
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → [DiGi\.Core\.Classes\.ConfigurationFile](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.configurationfile 'DiGi\.Core\.Classes\.ConfigurationFile') → PostgreSQLConfigurationFile

Implements [IPostgreSQLSerializableObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLSerializableObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLSerializableObject'), [IPostgreSQLObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLObject'), [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject'), [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject'), [DiGi\.Core\.Interfaces\.ICloneableObject&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1 'DiGi\.Core\.Interfaces\.ICloneableObject\`1')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1 'DiGi\.Core\.Interfaces\.ICloneableObject\`1'), [DiGi\.Core\.Interfaces\.ICloneableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject 'DiGi\.Core\.Interfaces\.ICloneableObject')
### Constructors

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.PostgreSQLConfigurationFile()'></a>

## PostgreSQLConfigurationFile\(\) Constructor

Initializes a new instance of the [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile') class\.

```csharp
public PostgreSQLConfigurationFile();
```

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.PostgreSQLConfigurationFile(DiGi.Core.Classes.ConfigurationFile)'></a>

## PostgreSQLConfigurationFile\(ConfigurationFile\) Constructor

Initializes a new instance of the [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile') class based on an existing configuration file\.

```csharp
public PostgreSQLConfigurationFile(DiGi.Core.Classes.ConfigurationFile? configurationFile);
```
#### Parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.PostgreSQLConfigurationFile(DiGi.Core.Classes.ConfigurationFile).configurationFile'></a>

`configurationFile` [DiGi\.Core\.Classes\.ConfigurationFile](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.configurationfile 'DiGi\.Core\.Classes\.ConfigurationFile')

The source configuration file to copy settings from\.

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.PostgreSQLConfigurationFile(System.Text.Json.Nodes.JsonObject)'></a>

## PostgreSQLConfigurationFile\(JsonObject\) Constructor

Initializes a new instance of the [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile') class using a JSON object\.

```csharp
public PostgreSQLConfigurationFile(System.Text.Json.Nodes.JsonObject? jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.PostgreSQLConfigurationFile(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The JSON object containing the configuration data\.
### Properties

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.Database'></a>

## PostgreSQLConfigurationFile\.Database Property

Gets or sets the name of the PostgreSQL database\.

```csharp
public string? Database { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.Directory'></a>

## PostgreSQLConfigurationFile\.Directory Property

Gets or sets the directory path associated with the PostgreSQL configuration\.

```csharp
public string? Directory { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.Host'></a>

## PostgreSQLConfigurationFile\.Host Property

Gets or sets the host address of the PostgreSQL server\.

```csharp
public string? Host { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.Password'></a>

## PostgreSQLConfigurationFile\.Password Property

Gets or sets the password for the PostgreSQL user\.

```csharp
public string? Password { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.Port'></a>

## PostgreSQLConfigurationFile\.Port Property

Gets or sets the port number used to connect to the PostgreSQL server\.

```csharp
public System.Nullable<int> Port { get; set; }
```

#### Property Value
[System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.Tablespace'></a>

## PostgreSQLConfigurationFile\.Tablespace Property

Gets or sets the tablespace name for the PostgreSQL database\.

```csharp
public string? Tablespace { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile.Username'></a>

## PostgreSQLConfigurationFile\.Username Property

Gets or sets the username for the PostgreSQL connection\.

```csharp
public string? Username { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverter_TObject_'></a>

## PostgreSQLConverter\<TObject\> Class

Base class for converting objects to and from PostgreSQL database format\.

```csharp
public abstract class PostgreSQLConverter<TObject> : DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter<TObject>, DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter, DiGi.PostgreSQL.Interfaces.IPostgreSQLObject, DiGi.Core.Interfaces.IObject
    where TObject : DiGi.Core.Interfaces.IObject
```
#### Type parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverter_TObject_.TObject'></a>

`TObject`

The type of object being converted, which must implement IObject\.

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → PostgreSQLConverter\<TObject\>

Implements [DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLConverter&lt;](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter_TObject_ 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLConverter\<TObject\>')[TObject](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverter_TObject_.TObject 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\<TObject\>\.TObject')[&gt;](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter_TObject_ 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLConverter\<TObject\>'), [IPostgreSQLConverter](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLConverter'), [IPostgreSQLObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLObject'), [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject')
### Constructors

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverter_TObject_.PostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## PostgreSQLConverter\(ConnectionData\) Constructor

Initializes a new instance of the PostgreSQLConverter class\.

```csharp
public PostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverter_TObject_.PostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection data to be used for database operations\.
### Properties

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverter_TObject_.ConnectionData'></a>

## PostgreSQLConverter\<TObject\>\.ConnectionData Property

Gets or sets the connection data used by the converter\.

```csharp
public DiGi.PostgreSQL.Classes.ConnectionData? ConnectionData { get; set; }
```

#### Property Value
[ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager'></a>

## PostgreSQLConverterManager Class

A non\-generic manager for PostgreSQL converters using the [IPostgreSQLConverter](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLConverter') interface\.

```csharp
public class PostgreSQLConverterManager : DiGi.PostgreSQL.Classes.PostgreSQLConverterManager<DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter>
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager&lt;](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_ 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager\<TPostgreSQLConverter\>')[IPostgreSQLConverter](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLConverter')[&gt;](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_ 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager\<TPostgreSQLConverter\>') → PostgreSQLConverterManager
### Constructors

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager.PostgreSQLConverterManager()'></a>

## PostgreSQLConverterManager\(\) Constructor

Initializes a new instance of the [PostgreSQLConverterManager](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager') class\.

```csharp
public PostgreSQLConverterManager();
```

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_'></a>

## PostgreSQLConverterManager\<TPostgreSQLConverter\> Class

Manages a collection of PostgreSQL converters and their associated configuration files\.

```csharp
public class PostgreSQLConverterManager<TPostgreSQLConverter> : DiGi.PostgreSQL.Interfaces.IPostgreSQLObject, DiGi.Core.Interfaces.IObject
    where TPostgreSQLConverter : DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter
```
#### Type parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TPostgreSQLConverter'></a>

`TPostgreSQLConverter`

The base type of the PostgreSQL converter to be managed\.

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → PostgreSQLConverterManager\<TPostgreSQLConverter\>

Derived  
↳ [PostgreSQLConverterManager](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager')

Implements [IPostgreSQLObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLObject'), [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject')
### Constructors

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.PostgreSQLConverterManager()'></a>

## PostgreSQLConverterManager\(\) Constructor

Initializes a new instance of the [PostgreSQLConverterManager&lt;TPostgreSQLConverter&gt;](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_ 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager\<TPostgreSQLConverter\>') class\.

```csharp
public PostgreSQLConverterManager();
```
### Methods

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.Add(TPostgreSQLConverter,DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile)'></a>

## PostgreSQLConverterManager\<TPostgreSQLConverter\>\.Add\(TPostgreSQLConverter, PostgreSQLConfigurationFile\) Method

Adds or updates a PostgreSQL converter and its associated configuration file in the manager\.

```csharp
public bool Add(TPostgreSQLConverter? postgreSQLConverter, DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile? postgreSQLConfigurationFile=null);
```
#### Parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.Add(TPostgreSQLConverter,DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile).postgreSQLConverter'></a>

`postgreSQLConverter` [TPostgreSQLConverter](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TPostgreSQLConverter 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager\<TPostgreSQLConverter\>\.TPostgreSQLConverter')

The PostgreSQL converter to add\.

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.Add(TPostgreSQLConverter,DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile).postgreSQLConfigurationFile'></a>

`postgreSQLConfigurationFile` [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile')

The optional configuration file associated with the converter\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the converter was successfully added or updated; otherwise, false\.

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.GetPostgreSQLConfigurationFile_UPostgreSQLConverter_()'></a>

## PostgreSQLConverterManager\<TPostgreSQLConverter\>\.GetPostgreSQLConfigurationFile\<UPostgreSQLConverter\>\(\) Method

Retrieves the configuration file for a specific type of PostgreSQL converter\.

```csharp
public DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile? GetPostgreSQLConfigurationFile<UPostgreSQLConverter>()
    where UPostgreSQLConverter : TPostgreSQLConverter;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.GetPostgreSQLConfigurationFile_UPostgreSQLConverter_().UPostgreSQLConverter'></a>

`UPostgreSQLConverter`

The specific type of the PostgreSQL converter\.

#### Returns
[PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile')  
The associated [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile'), or null if not found\.

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.GetPostgreSQLConverter_UPostgreSQLConverter_()'></a>

## PostgreSQLConverterManager\<TPostgreSQLConverter\>\.GetPostgreSQLConverter\<UPostgreSQLConverter\>\(\) Method

Retrieves a specific instance of a PostgreSQL converter by its type\.

```csharp
public UPostgreSQLConverter? GetPostgreSQLConverter<UPostgreSQLConverter>()
    where UPostgreSQLConverter : TPostgreSQLConverter;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.GetPostgreSQLConverter_UPostgreSQLConverter_().UPostgreSQLConverter'></a>

`UPostgreSQLConverter`

The specific type of the PostgreSQL converter to retrieve\.

#### Returns
[UPostgreSQLConverter](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.GetPostgreSQLConverter_UPostgreSQLConverter_().UPostgreSQLConverter 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager\<TPostgreSQLConverter\>\.GetPostgreSQLConverter\<UPostgreSQLConverter\>\(\)\.UPostgreSQLConverter')  
The [UPostgreSQLConverter](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.GetPostgreSQLConverter_UPostgreSQLConverter_().UPostgreSQLConverter 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager\<TPostgreSQLConverter\>\.GetPostgreSQLConverter\<UPostgreSQLConverter\>\(\)\.UPostgreSQLConverter') instance, or null if not found\.

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.GetPostgreSQLConverters_UPostgreSQLConverter_()'></a>

## PostgreSQLConverterManager\<TPostgreSQLConverter\>\.GetPostgreSQLConverters\<UPostgreSQLConverter\>\(\) Method

Retrieves all registered converters of a specific type\.

```csharp
public System.Collections.Generic.List<UPostgreSQLConverter> GetPostgreSQLConverters<UPostgreSQLConverter>()
    where UPostgreSQLConverter : TPostgreSQLConverter;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.GetPostgreSQLConverters_UPostgreSQLConverter_().UPostgreSQLConverter'></a>

`UPostgreSQLConverter`

The specific type of the PostgreSQL converters to retrieve\.

#### Returns
[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UPostgreSQLConverter](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.GetPostgreSQLConverters_UPostgreSQLConverter_().UPostgreSQLConverter 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager\<TPostgreSQLConverter\>\.GetPostgreSQLConverters\<UPostgreSQLConverter\>\(\)\.UPostgreSQLConverter')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')  
A list of [UPostgreSQLConverter](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.GetPostgreSQLConverters_UPostgreSQLConverter_().UPostgreSQLConverter 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager\<TPostgreSQLConverter\>\.GetPostgreSQLConverters\<UPostgreSQLConverter\>\(\)\.UPostgreSQLConverter') instances\.

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.IsAvailable_UPostgreSQLConverter_()'></a>

## PostgreSQLConverterManager\<TPostgreSQLConverter\>\.IsAvailable\<UPostgreSQLConverter\>\(\) Method

Checks if the database associated with a specific converter type is available\.

```csharp
public bool IsAvailable<UPostgreSQLConverter>()
    where UPostgreSQLConverter : TPostgreSQLConverter;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.IsAvailable_UPostgreSQLConverter_().UPostgreSQLConverter'></a>

`UPostgreSQLConverter`

The specific type of the PostgreSQL converter\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the configuration exists and the database is available; otherwise, false\.

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TryCreateDatabase_UPostgreSQLConverter_()'></a>

## PostgreSQLConverterManager\<TPostgreSQLConverter\>\.TryCreateDatabase\<UPostgreSQLConverter\>\(\) Method

Attempts to create a database using the configuration associated with a specific converter type\.

```csharp
public System.Threading.Tasks.Task<bool> TryCreateDatabase<UPostgreSQLConverter>()
    where UPostgreSQLConverter : TPostgreSQLConverter;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TryCreateDatabase_UPostgreSQLConverter_().UPostgreSQLConverter'></a>

`UPostgreSQLConverter`

The specific type of the PostgreSQL converter\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task representing the asynchronous operation, containing true if the database was created successfully; otherwise, false\.

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TryGetPostgreSQLConfigurationFile_UPostgreSQLConverter_(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile)'></a>

## PostgreSQLConverterManager\<TPostgreSQLConverter\>\.TryGetPostgreSQLConfigurationFile\<UPostgreSQLConverter\>\(PostgreSQLConfigurationFile\) Method

Attempts to retrieve the configuration file for a specific converter type\.

```csharp
public bool TryGetPostgreSQLConfigurationFile<UPostgreSQLConverter>(out DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile? postgreSQLConfigurationFile)
    where UPostgreSQLConverter : TPostgreSQLConverter;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TryGetPostgreSQLConfigurationFile_UPostgreSQLConverter_(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile).UPostgreSQLConverter'></a>

`UPostgreSQLConverter`

The specific type of the PostgreSQL converter\.
#### Parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TryGetPostgreSQLConfigurationFile_UPostgreSQLConverter_(DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile).postgreSQLConfigurationFile'></a>

`postgreSQLConfigurationFile` [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile')

When this method returns, contains the configuration file if found; otherwise, null\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the configuration file was successfully retrieved; otherwise, false\.

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TryGetPostgreSQLConverter_UPostgreSQLConverter_(UPostgreSQLConverter)'></a>

## PostgreSQLConverterManager\<TPostgreSQLConverter\>\.TryGetPostgreSQLConverter\<UPostgreSQLConverter\>\(UPostgreSQLConverter\) Method

Attempts to retrieve a converter instance of a specific type\.

```csharp
public bool TryGetPostgreSQLConverter<UPostgreSQLConverter>(out UPostgreSQLConverter? postgreSQLConverter)
    where UPostgreSQLConverter : TPostgreSQLConverter;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TryGetPostgreSQLConverter_UPostgreSQLConverter_(UPostgreSQLConverter).UPostgreSQLConverter'></a>

`UPostgreSQLConverter`

The specific type of the PostgreSQL converter\.
#### Parameters

<a name='DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TryGetPostgreSQLConverter_UPostgreSQLConverter_(UPostgreSQLConverter).postgreSQLConverter'></a>

`postgreSQLConverter` [UPostgreSQLConverter](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_.TryGetPostgreSQLConverter_UPostgreSQLConverter_(UPostgreSQLConverter).UPostgreSQLConverter 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager\<TPostgreSQLConverter\>\.TryGetPostgreSQLConverter\<UPostgreSQLConverter\>\(UPostgreSQLConverter\)\.UPostgreSQLConverter')

When this method returns, contains the converter instance if found; otherwise, null\.

#### Returns
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')  
True if the converter was successfully retrieved; otherwise, false\.

<a name='DiGi.PostgreSQL.Classes.ReferenceGeneratingEventArgs'></a>

## ReferenceGeneratingEventArgs Class

Provides data for events that occur during reference generation\.

```csharp
public abstract class ReferenceGeneratingEventArgs : System.EventArgs
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [System\.EventArgs](https://learn.microsoft.com/en-us/dotnet/api/system.eventargs 'System\.EventArgs') → ReferenceGeneratingEventArgs
### Constructors

<a name='DiGi.PostgreSQL.Classes.ReferenceGeneratingEventArgs.ReferenceGeneratingEventArgs(object)'></a>

## ReferenceGeneratingEventArgs\(object\) Constructor

Initializes a new instance of the [ReferenceGeneratingEventArgs](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ReferenceGeneratingEventArgs 'DiGi\.PostgreSQL\.Classes\.ReferenceGeneratingEventArgs') class\.

```csharp
public ReferenceGeneratingEventArgs(object? item);
```
#### Parameters

<a name='DiGi.PostgreSQL.Classes.ReferenceGeneratingEventArgs.ReferenceGeneratingEventArgs(object).item'></a>

`item` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The item associated with the reference generation process\.
### Fields

<a name='DiGi.PostgreSQL.Classes.ReferenceGeneratingEventArgs.handled'></a>

## ReferenceGeneratingEventArgs\.handled Field

Indicates whether the event has been handled\.

```csharp
protected bool handled;
```

#### Field Value
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')
### Properties

<a name='DiGi.PostgreSQL.Classes.ReferenceGeneratingEventArgs.Handled'></a>

## ReferenceGeneratingEventArgs\.Handled Property

Gets a value indicating whether the event has been handled\.

```csharp
public bool Handled { get; }
```

#### Property Value
[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')

<a name='DiGi.PostgreSQL.Classes.ReferenceGeneratingEventArgs.Item'></a>

## ReferenceGeneratingEventArgs\.Item Property

Gets the item associated with the reference generation process\.

```csharp
public object? Item { get; }
```

#### Property Value
[System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')