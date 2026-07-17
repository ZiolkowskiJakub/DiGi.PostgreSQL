#### [DiGi\.PostgreSQL\.PartitionReference](DiGi.PostgreSQL.PartitionReference.Overview.md 'DiGi\.PostgreSQL\.PartitionReference\.Overview')

## DiGi\.PostgreSQL\.PartitionReference\.Constants Namespace
### Classes

<a name='DiGi.PostgreSQL.PartitionReference.Constants.ReferenceKind'></a>

## ReferenceKind Class

Discriminator tokens for the reference types defined in DiGi\.PostgreSQL\.PartitionReference\.

These values are a persisted contract: they are written into stored reference strings, so they are
            append-only. Renaming one silently invalidates every string already stored in that format. A token must be
            unique across every repository, and must contain neither a comma (which would make it parse as a full type
            name) nor a colon.

This class is deliberately NOT named Reference. It replaces a Constants/Reference.cs that declared its
            own `Separator = "->"` and, by innermost-namespace lookup, silently shadowed
            DiGi.Core.Constants.Reference for every type in this namespace - which is why the partition references used a
            different grammar from the rest of the codebase. Do not re-create a local Constants.Reference here.

```csharp
public static class ReferenceKind
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → ReferenceKind
### Fields

<a name='DiGi.PostgreSQL.PartitionReference.Constants.ReferenceKind.Partition'></a>

## ReferenceKind\.Partition Field

Discriminator for [PartitionReference](DiGi.PostgreSQL.PartitionReference.Classes.md#DiGi.PostgreSQL.PartitionReference.Classes.PartitionReference 'DiGi\.PostgreSQL\.PartitionReference\.Classes\.PartitionReference')\.

```csharp
public const string Partition = "Partition";
```

#### Field Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')