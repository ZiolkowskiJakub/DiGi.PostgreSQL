#### [DiGi\.PostgreSQL\.Table](DiGi.PostgreSQL.Table.Overview.md 'DiGi\.PostgreSQL\.Table\.Overview')

## DiGi\.PostgreSQL\.Table\.Constants Namespace
### Classes

<a name='DiGi.PostgreSQL.Table.Constants.ColumnName'></a>

## ColumnName Class

Provides constant values for column names the table converter writes into its own statements\.

```csharp
public static class ColumnName
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → ColumnName
### Fields

<a name='DiGi.PostgreSQL.Table.Constants.ColumnName.PhysicalPosition'></a>

## ColumnName\.PhysicalPosition Field

The alias of the extra column carrying a row's physical position \(`ctid` as text\) in a physical\-order read\. It is read by the converter and never added to the pulled table\.

```csharp
public const string PhysicalPosition = "__digi_physical_position";
```

#### Field Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Constants.TableName'></a>

## TableName Class

Provides constant values for PostgreSQL table names\.

```csharp
public static class TableName
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → TableName
### Fields

<a name='DiGi.PostgreSQL.Table.Constants.TableName.Columns'></a>

## TableName\.Columns Field

The name of the table that contains column information\.

```csharp
public const string Columns = "columns";
```

#### Field Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')