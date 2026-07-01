#### [DiGi\.PostgreSQL](index.md 'index')

## DiGi\.PostgreSQL\.Interfaces Namespace
### Interfaces

<a name='DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter'></a>

## IPostgreSQLConverter Interface

Defines the base contract for a PostgreSQL converter\.

```csharp
public interface IPostgreSQLConverter : DiGi.PostgreSQL.Interfaces.IPostgreSQLObject, DiGi.Core.Interfaces.IObject
```

Derived  
↳ [PostgreSQLConverter&lt;TObject&gt;](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverter_TObject_ 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\<TObject\>')  
↳ [IPostgreSQLConverter&lt;TObject&gt;](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter_TObject_ 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLConverter\<TObject\>')

Implements [IPostgreSQLObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLObject'), [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject')

<a name='DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter_TObject_'></a>

## IPostgreSQLConverter\<TObject\> Interface

Defines the contract for a PostgreSQL converter that handles a specific object type\.

```csharp
public interface IPostgreSQLConverter<TObject> : DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter, DiGi.PostgreSQL.Interfaces.IPostgreSQLObject, DiGi.Core.Interfaces.IObject
    where TObject : DiGi.Core.Interfaces.IObject
```
#### Type parameters

<a name='DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter_TObject_.TObject'></a>

`TObject`

The type of object to be converted, which must implement [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject')\.

Derived  
↳ [PostgreSQLConverter&lt;TObject&gt;](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverter_TObject_ 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\<TObject\>')

Implements [IPostgreSQLConverter](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLConverter'), [IPostgreSQLObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLObject'), [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject')

<a name='DiGi.PostgreSQL.Interfaces.IPostgreSQLObject'></a>

## IPostgreSQLObject Interface

Represents an object within the PostgreSQL database context, extending the base object functionality\.

```csharp
public interface IPostgreSQLObject : DiGi.Core.Interfaces.IObject
```

Derived  
↳ [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')  
↳ [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')  
↳ [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile')  
↳ [PostgreSQLConverter&lt;TObject&gt;](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverter_TObject_ 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\<TObject\>')  
↳ [PostgreSQLConverterManager&lt;TPostgreSQLConverter&gt;](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConverterManager_TPostgreSQLConverter_ 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverterManager\<TPostgreSQLConverter\>')  
↳ [IPostgreSQLConverter](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLConverter')  
↳ [IPostgreSQLConverter&lt;TObject&gt;](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLConverter_TObject_ 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLConverter\<TObject\>')  
↳ [IPostgreSQLSerializableObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLSerializableObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLSerializableObject')

Implements [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject')

<a name='DiGi.PostgreSQL.Interfaces.IPostgreSQLSerializableObject'></a>

## IPostgreSQLSerializableObject Interface

Defines the contract for objects that are compatible with PostgreSQL storage and can be serialized to and from JSON\.

```csharp
public interface IPostgreSQLSerializableObject : DiGi.PostgreSQL.Interfaces.IPostgreSQLObject, DiGi.Core.Interfaces.IObject, DiGi.Core.Interfaces.ISerializableObject, DiGi.Core.Interfaces.ICloneableObject<DiGi.Core.Interfaces.ISerializableObject>, DiGi.Core.Interfaces.ICloneableObject
```

Derived  
↳ [ConnectionData](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.ConnectionData 'DiGi\.PostgreSQL\.Classes\.ConnectionData')  
↳ [Partition](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.Partition 'DiGi\.PostgreSQL\.Classes\.Partition')  
↳ [PostgreSQLConfigurationFile](DiGi.PostgreSQL.Classes.md#DiGi.PostgreSQL.Classes.PostgreSQLConfigurationFile 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConfigurationFile')

Implements [IPostgreSQLObject](DiGi.PostgreSQL.Interfaces.md#DiGi.PostgreSQL.Interfaces.IPostgreSQLObject 'DiGi\.PostgreSQL\.Interfaces\.IPostgreSQLObject'), [DiGi\.Core\.Interfaces\.IObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iobject 'DiGi\.Core\.Interfaces\.IObject'), [DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject'), [DiGi\.Core\.Interfaces\.ICloneableObject&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1 'DiGi\.Core\.Interfaces\.ICloneableObject\`1')[DiGi\.Core\.Interfaces\.ISerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.iserializableobject 'DiGi\.Core\.Interfaces\.ISerializableObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject-1 'DiGi\.Core\.Interfaces\.ICloneableObject\`1'), [DiGi\.Core\.Interfaces\.ICloneableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.interfaces.icloneableobject 'DiGi\.Core\.Interfaces\.ICloneableObject')