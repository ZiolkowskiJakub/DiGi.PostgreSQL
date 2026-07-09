#### [DiGi\.PostgreSQL\.UniqueReference](DiGi.PostgreSQL.UniqueReference.Overview.md 'DiGi\.PostgreSQL\.UniqueReference\.Overview')

## DiGi\.PostgreSQL\.UniqueReference\.Delegates Namespace
### Delegates

<a name='DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs)'></a>

## UniqueIdReferenceGeneratingEventHandler\(object, UniqueIdReferenceGeneratingEventArgs\) Delegate

Represents the method that will handle the event when a unique ID reference is being generated\.

```csharp
public delegate void UniqueIdReferenceGeneratingEventHandler(object sender, DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs e);
```
#### Parameters

<a name='DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs).sender'></a>

`sender` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The source of the event\.

<a name='DiGi.PostgreSQL.UniqueReference.Delegates.UniqueIdReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs).e'></a>

`e` [UniqueIdReferenceGeneratingEventArgs](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueIdReferenceGeneratingEventArgs')

A [UniqueIdReferenceGeneratingEventArgs](DiGi.PostgreSQL.UniqueReference.Classes.md#DiGi.PostgreSQL.UniqueReference.Classes.UniqueIdReferenceGeneratingEventArgs 'DiGi\.PostgreSQL\.UniqueReference\.Classes\.UniqueIdReferenceGeneratingEventArgs') object that contains the event data\.