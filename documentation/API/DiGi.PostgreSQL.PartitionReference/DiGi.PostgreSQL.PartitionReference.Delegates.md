#### [DiGi\.PostgreSQL\.PartitionReference](index.md 'index')

## DiGi\.PostgreSQL\.PartitionReference\.Delegates Namespace
### Delegates

<a name='DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs)'></a>

## PartitionReferenceGeneratingEventHandler\(object, PartitionReferenceGeneratingEventArgs\) Delegate

Represents the method that will handle the event when a partition reference is being generated\.

```csharp
public delegate void PartitionReferenceGeneratingEventHandler(object sender, DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs e);
```
#### Parameters

<a name='DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs).sender'></a>

`sender` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The source of the event\.

<a name='DiGi.PostgreSQL.PartitionReference.Delegates.PartitionReferenceGeneratingEventHandler(object,DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs).e'></a>

`e` [PartitionReferenceGeneratingEventArgs](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferenceGeneratingEventArgs')

A [PartitionReferenceGeneratingEventArgs](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReferenceGeneratingEventArgs 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReferenceGeneratingEventArgs') that contains the event data\.