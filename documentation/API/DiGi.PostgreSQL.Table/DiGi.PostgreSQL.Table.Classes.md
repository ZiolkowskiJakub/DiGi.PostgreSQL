#### [DiGi\.PostgreSQL\.Table](DiGi.PostgreSQL.Table.Overview.md 'DiGi\.PostgreSQL\.Table\.Overview')

## DiGi\.PostgreSQL\.Table\.Classes Namespace
### Classes

<a name='DiGi.PostgreSQL.Table.Classes.Column'></a>

## Column Class

Represents a column in a PostgreSQL table\.

```csharp
public class Column : DiGi.Core.Classes.SerializableObject
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → Column
### Constructors

<a name='DiGi.PostgreSQL.Table.Classes.Column.Column()'></a>

## Column\(\) Constructor

Initializes a new instance of the Column class\.

```csharp
public Column();
```

<a name='DiGi.PostgreSQL.Table.Classes.Column.Column(DiGi.PostgreSQL.Table.Classes.Column)'></a>

## Column\(Column\) Constructor

Initializes a new instance of the [Column](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Column 'DiGi\.PostgreSQL\.Table\.Classes\.Column') class by cloning another Column instance\.

```csharp
public Column(DiGi.PostgreSQL.Table.Classes.Column? column);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.Column.Column(DiGi.PostgreSQL.Table.Classes.Column).column'></a>

`column` [Column](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Column 'DiGi\.PostgreSQL\.Table\.Classes\.Column')

The source [Column](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Column 'DiGi\.PostgreSQL\.Table\.Classes\.Column') instance to copy values from\.

<a name='DiGi.PostgreSQL.Table.Classes.Column.Column(System.Text.Json.Nodes.JsonObject)'></a>

## Column\(JsonObject\) Constructor

Initializes a new instance of the [Column](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Column 'DiGi\.PostgreSQL\.Table\.Classes\.Column') class from a [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')\.

```csharp
public Column(System.Text.Json.Nodes.JsonObject? jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.Column.Column(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject') used to initialize the column properties\.
### Properties

<a name='DiGi.PostgreSQL.Table.Classes.Column.Category'></a>

## Column\.Category Property

Gets or sets the category of the column\.

```csharp
public string? Category { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.Column.DataType'></a>

## Column\.DataType Property

Gets or sets the values DataType for the column across different contexts\.

```csharp
public System.Nullable<DiGi.Core.Enums.DataType> DataType { get; set; }
```

#### Property Value
[System\.Nullable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')[DiGi\.Core\.Enums\.DataType](https://learn.microsoft.com/en-us/dotnet/api/digi.core.enums.datatype 'DiGi\.Core\.Enums\.DataType')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.nullable-1 'System\.Nullable\`1')

<a name='DiGi.PostgreSQL.Table.Classes.Column.Description'></a>

## Column\.Description Property

Gets or sets the description of the column reference\.

```csharp
public string? Description { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.Column.Index'></a>

## Column\.Index Property

Gets or sets the unique identifier of the column\.

```csharp
public int Index { get; set; }
```

#### Property Value
[System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

<a name='DiGi.PostgreSQL.Table.Classes.Column.Name'></a>

## Column\.Name Property

Gets or sets the name of the column\.

```csharp
public string? Name { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.Column.UniqueId'></a>

## Column\.UniqueId Property

Gets or sets the unique identifier for the data column across different contexts\.

```csharp
public string? UniqueId { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference'></a>

## ColumnReference Class

Represents a reference to a column in a PostgreSQL table\.

```csharp
public class ColumnReference : DiGi.Core.Classes.SerializableObject
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → ColumnReference
### Constructors

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.ColumnReference()'></a>

## ColumnReference\(\) Constructor

Initializes a new instance of the ColumnReference class\.

```csharp
public ColumnReference();
```

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.ColumnReference(DiGi.PostgreSQL.Table.Classes.ColumnReference)'></a>

## ColumnReference\(ColumnReference\) Constructor

Initializes a new instance of the [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') class by copying another [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') instance\.

```csharp
public ColumnReference(DiGi.PostgreSQL.Table.Classes.ColumnReference? columnReference);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.ColumnReference(DiGi.PostgreSQL.Table.Classes.ColumnReference).columnReference'></a>

`columnReference` [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference')

The column reference to copy\.

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.ColumnReference(System.Text.Json.Nodes.JsonObject)'></a>

## ColumnReference\(JsonObject\) Constructor

Initializes a new instance of the [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') class from a [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')\.

```csharp
public ColumnReference(System.Text.Json.Nodes.JsonObject? jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.ColumnReference(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The JSON object to initialize from\.
### Properties

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.Category'></a>

## ColumnReference\.Category Property

Gets or sets the category of the column reference\.

```csharp
public string? Category { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.Description'></a>

## ColumnReference\.Description Property

Gets or sets the description of the column reference\.

```csharp
public string? Description { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.Id'></a>

## ColumnReference\.Id Property

Gets or sets the unique identifier of the column\.

```csharp
public int Id { get; set; }
```

#### Property Value
[System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.Name'></a>

## ColumnReference\.Name Property

Gets or sets the name of the column\.

```csharp
public string? Name { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.TableName'></a>

## ColumnReference\.TableName Property

Gets or sets the name of the table containing the column\.

```csharp
public string? TableName { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.ColumnReference.UniqueId'></a>

## ColumnReference\.UniqueId Property

Gets or sets the unique identifier for the column reference across different contexts\.

```csharp
public string? UniqueId { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.FilterCondition'></a>

## FilterCondition Class

Represents a single comparison filter condition on a database column\.

```csharp
public class FilterCondition
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → FilterCondition
### Properties

<a name='DiGi.PostgreSQL.Table.Classes.FilterCondition.ColumnUniqueId'></a>

## FilterCondition\.ColumnUniqueId Property

Gets or sets the unique identifier of the column to filter\.

```csharp
public string? ColumnUniqueId { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.FilterCondition.FilterOperator'></a>

## FilterCondition\.FilterOperator Property

Gets or sets the comparison operator to apply\.

```csharp
public DiGi.PostgreSQL.Table.Enums.FilterOperator FilterOperator { get; set; }
```

#### Property Value
[FilterOperator](DiGi.PostgreSQL.Table.Enums.md#DiGi.PostgreSQL.Table.Enums.FilterOperator 'DiGi\.PostgreSQL\.Table\.Enums\.FilterOperator')

<a name='DiGi.PostgreSQL.Table.Classes.FilterCondition.Value'></a>

## FilterCondition\.Value Property

Gets or sets the value to compare against\. For list operators like In or NotIn, this should be a collection of values\.

```csharp
public object? Value { get; set; }
```

#### Property Value
[System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

<a name='DiGi.PostgreSQL.Table.Classes.FilterGroup'></a>

## FilterGroup Class

Represents a group of filter conditions and sub\-groups combined by a logical operator\.

```csharp
public class FilterGroup
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → FilterGroup
### Properties

<a name='DiGi.PostgreSQL.Table.Classes.FilterGroup.FilterConditions'></a>

## FilterGroup\.FilterConditions Property

Gets or sets the list of individual filter conditions within this group\.

```csharp
public System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.FilterCondition> FilterConditions { get; set; }
```

#### Property Value
[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[FilterCondition](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterCondition 'DiGi\.PostgreSQL\.Table\.Classes\.FilterCondition')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')

<a name='DiGi.PostgreSQL.Table.Classes.FilterGroup.FilterGroups'></a>

## FilterGroup\.FilterGroups Property

Gets or sets the list of sub\-groups nested under this group\.

```csharp
public System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.FilterGroup> FilterGroups { get; set; }
```

#### Property Value
[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')

<a name='DiGi.PostgreSQL.Table.Classes.FilterGroup.LogicalOperator'></a>

## FilterGroup\.LogicalOperator Property

Gets or sets the logical operator \(AND or OR\) used to combine the elements within this group\.

```csharp
public DiGi.PostgreSQL.Table.Enums.FilterLogicalOperator LogicalOperator { get; set; }
```

#### Property Value
[FilterLogicalOperator](DiGi.PostgreSQL.Table.Enums.md#DiGi.PostgreSQL.Table.Enums.FilterLogicalOperator 'DiGi\.PostgreSQL\.Table\.Enums\.FilterLogicalOperator')

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningOptions_UColumn_'></a>

## PartitioningOptions\<UColumn\> Class

Options for configuring the partitioning of a table\.

```csharp
public class PartitioningOptions<UColumn>
    where UColumn : DiGi.Core.IO.Table.Interfaces.IColumn
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningOptions_UColumn_.UColumn'></a>

`UColumn`

The type of the column used for partitioning, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn')\.

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → PartitioningOptions\<UColumn\>
### Properties

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningOptions_UColumn_.Column'></a>

## PartitioningOptions\<UColumn\>\.Column Property

Gets or sets the column used as the partition key\.

```csharp
public UColumn? Column { get; set; }
```

#### Property Value
[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningOptions_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningOptions\<UColumn\>\.UColumn')

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningOptions_UColumn_.DefaultSuffix'></a>

## PartitioningOptions\<UColumn\>\.DefaultSuffix Property

Gets or sets the default suffix applied to partition table names\.

```csharp
public string? DefaultSuffix { get; set; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningOptions_UColumn_.PartitioningRule'></a>

## PartitioningOptions\<UColumn\>\.PartitioningRule Property

Gets or sets the rule used for partitioning the table\.

```csharp
public DiGi.PostgreSQL.Table.Classes.PartitioningRule? PartitioningRule { get; set; }
```

#### Property Value
[PartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningRule')

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningRule'></a>

## PartitioningRule Class

Represents an abstract base class for defining partitioning rules in a PostgreSQL table\.

```csharp
public abstract class PartitioningRule : DiGi.Core.Classes.SerializableObject
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → PartitioningRule

Derived  
↳ [RangePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule')  
↳ [ValuePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.ValuePartitioningRule')
### Constructors

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningRule.PartitioningRule()'></a>

## PartitioningRule\(\) Constructor

Initializes a new instance of the [PartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningRule') class\.

```csharp
public PartitioningRule();
```

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningRule.PartitioningRule(DiGi.PostgreSQL.Table.Classes.PartitioningRule)'></a>

## PartitioningRule\(PartitioningRule\) Constructor

Initializes a new instance of the [PartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningRule') class by copying an existing partitioning rule\.

```csharp
public PartitioningRule(DiGi.PostgreSQL.Table.Classes.PartitioningRule partitioningRule);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningRule.PartitioningRule(DiGi.PostgreSQL.Table.Classes.PartitioningRule).partitioningRule'></a>

`partitioningRule` [PartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningRule')

The source partitioning rule to copy from\.

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningRule.PartitioningRule(System.Text.Json.Nodes.JsonObject)'></a>

## PartitioningRule\(JsonObject\) Constructor

Initializes a new instance of the [PartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningRule') class using a JSON object\.

```csharp
public PartitioningRule(System.Text.Json.Nodes.JsonObject jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.PartitioningRule.PartitioningRule(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The JSON object containing the partitioning rule data\.

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule'></a>

## RangePartitioningRule Class

Represents a base rule for range\-based partitioning in PostgreSQL\.

```csharp
public abstract class RangePartitioningRule : DiGi.PostgreSQL.Table.Classes.PartitioningRule
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → [PartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningRule') → RangePartitioningRule

Derived  
↳ [RangePartitioningRule&lt;TNumber&gt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_ 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule\<TNumber\>')
### Constructors

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule.RangePartitioningRule()'></a>

## RangePartitioningRule\(\) Constructor

Initializes a new instance of the [RangePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule') class\.

```csharp
public RangePartitioningRule();
```

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule.RangePartitioningRule(DiGi.PostgreSQL.Table.Classes.RangePartitioningRule)'></a>

## RangePartitioningRule\(RangePartitioningRule\) Constructor

Initializes a new instance of the [RangePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule') class using an existing rule\.

```csharp
public RangePartitioningRule(DiGi.PostgreSQL.Table.Classes.RangePartitioningRule rangePartitioningRule);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule.RangePartitioningRule(DiGi.PostgreSQL.Table.Classes.RangePartitioningRule).rangePartitioningRule'></a>

`rangePartitioningRule` [RangePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule')

The source range partitioning rule to copy from\.

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule.RangePartitioningRule(System.Text.Json.Nodes.JsonObject)'></a>

## RangePartitioningRule\(JsonObject\) Constructor

Initializes a new instance of the [RangePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule') class from a JSON object\.

```csharp
public RangePartitioningRule(System.Text.Json.Nodes.JsonObject jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule.RangePartitioningRule(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The JSON object containing the rule configuration\.

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_'></a>

## RangePartitioningRule\<TNumber\> Class

Represents a range partitioning rule for a specific numeric type\.

```csharp
public class RangePartitioningRule<TNumber> : DiGi.PostgreSQL.Table.Classes.RangePartitioningRule
    where TNumber : System.Numerics.INumber<TNumber>
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.TNumber'></a>

`TNumber`

The numeric type used for the partition ranges\.

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → [PartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningRule') → [RangePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule') → RangePartitioningRule\<TNumber\>
### Constructors

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.RangePartitioningRule()'></a>

## RangePartitioningRule\(\) Constructor

Initializes a new instance of the [RangePartitioningRule&lt;TNumber&gt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_ 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule\<TNumber\>') class\.

```csharp
public RangePartitioningRule();
```

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.RangePartitioningRule(DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_)'></a>

## RangePartitioningRule\(RangePartitioningRule\<TNumber\>\) Constructor

Initializes a new instance of the [RangePartitioningRule&lt;TNumber&gt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_ 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule\<TNumber\>') class using an existing rule\.

```csharp
public RangePartitioningRule(DiGi.PostgreSQL.Table.Classes.RangePartitioningRule<TNumber> rangePartitioningRule);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.RangePartitioningRule(DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_).rangePartitioningRule'></a>

`rangePartitioningRule` [DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule&lt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_ 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule\<TNumber\>')[TNumber](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.TNumber 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule\<TNumber\>\.TNumber')[&gt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_ 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule\<TNumber\>')

The source range partitioning rule to copy from\.

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.RangePartitioningRule(System.Text.Json.Nodes.JsonObject)'></a>

## RangePartitioningRule\(JsonObject\) Constructor

Initializes a new instance of the [RangePartitioningRule&lt;TNumber&gt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_ 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule\<TNumber\>') class from a JSON object\.

```csharp
public RangePartitioningRule(System.Text.Json.Nodes.JsonObject jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.RangePartitioningRule(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The JSON object containing the rule configuration\.
### Properties

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.Ranges'></a>

## RangePartitioningRule\<TNumber\>\.Ranges Property

Gets or sets the list of ranges defined for this partitioning rule\.

```csharp
public System.Collections.Generic.List<DiGi.Core.Classes.Range<TNumber>>? Ranges { get; set; }
```

#### Property Value
[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[DiGi\.Core\.Classes\.Range&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.range-1 'DiGi\.Core\.Classes\.Range\`1')[TNumber](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.TNumber 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule\<TNumber\>\.TNumber')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.range-1 'DiGi\.Core\.Classes\.Range\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')
### Methods

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.GetPartitionSufix(TNumber)'></a>

## RangePartitioningRule\<TNumber\>\.GetPartitionSufix\(TNumber\) Method

Gets the partition suffix for a given numeric value based on the defined ranges\.

```csharp
public string? GetPartitionSufix(TNumber value);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.GetPartitionSufix(TNumber).value'></a>

`value` [TNumber](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.TNumber 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule\<TNumber\>\.TNumber')

The numeric value to evaluate\.

#### Returns
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')  
A string representing the partition suffix if a matching range is found; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.ToString(TNumber)'></a>

## RangePartitioningRule\<TNumber\>\.ToString\(TNumber\) Method

Converts a numeric value to a parametric string representation, replacing the negative sign '\-' with 'm' and the decimal separator '\.' with 'p'\.

```csharp
public static string ToString(TNumber value);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.ToString(TNumber).value'></a>

`value` [TNumber](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.RangePartitioningRule_TNumber_.TNumber 'DiGi\.PostgreSQL\.Table\.Classes\.RangePartitioningRule\<TNumber\>\.TNumber')

The numeric value to be formatted\.

#### Returns
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')  
A string representing the numeric value in the parametric format\.

<a name='DiGi.PostgreSQL.Table.Classes.Table'></a>

## Table Class

Represents a PostgreSQL table structure and its data\.

```csharp
public class Table : DiGi.Core.Classes.SerializableObject
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → Table
### Constructors

<a name='DiGi.PostgreSQL.Table.Classes.Table.Table()'></a>

## Table\(\) Constructor

Initializes a new instance of the Table class\.

```csharp
public Table();
```

<a name='DiGi.PostgreSQL.Table.Classes.Table.Table(DiGi.PostgreSQL.Table.Classes.Table)'></a>

## Table\(Table\) Constructor

Initializes a new instance of the [Table](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Table 'DiGi\.PostgreSQL\.Table\.Classes\.Table') class by cloning an existing table instance\.

```csharp
public Table(DiGi.PostgreSQL.Table.Classes.Table? table);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.Table.Table(DiGi.PostgreSQL.Table.Classes.Table).table'></a>

`table` [Table](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Table 'DiGi\.PostgreSQL\.Table\.Classes\.Table')

The source [Table](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Table 'DiGi\.PostgreSQL\.Table\.Classes\.Table') instance to clone from, or null to initialize an empty table\.

<a name='DiGi.PostgreSQL.Table.Classes.Table.Table(System.Text.Json.Nodes.JsonObject)'></a>

## Table\(JsonObject\) Constructor

Initializes a new instance of the [Table](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Table 'DiGi\.PostgreSQL\.Table\.Classes\.Table') class using the provided [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')\.

```csharp
public Table(System.Text.Json.Nodes.JsonObject? jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.Table.Table(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject') containing the table data, or null to initialize an empty table\.
### Properties

<a name='DiGi.PostgreSQL.Table.Classes.Table.Columns'></a>

## Table\.Columns Property

Gets or sets the list of columns in the table\.

```csharp
public System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.Column?> Columns { get; set; }
```

#### Property Value
[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[Column](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.Column 'DiGi\.PostgreSQL\.Table\.Classes\.Column')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')

### Example
\[
  \{ "Name": "Reference", "UniqueId": "reference", "Category": "Administrative" \},
  \{ "Name": "County Id", "UniqueId": "count\_id", "Category": "Administrative" \},
  \{ "Name": "Floor area", "UniqueId": "floor\_area", "Category": "Shape descriptors" \}
\]

<a name='DiGi.PostgreSQL.Table.Classes.Table.Rows'></a>

## Table\.Rows Property

Gets or sets the data values in rows stored in the table\.

```csharp
public System.Collections.Generic.List<object?[]> Rows { get; set; }
```

#### Property Value
[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')[\[\]](https://learn.microsoft.com/en-us/dotnet/api/system.array 'System\.Array')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')

### Example
\[
  \[ "a71b3f91\-819f\-489a\-93c3\-a850948c60af", 10365, 308\.38 \],
  \[ "b82c4g02\-920g\-590b\-04d4\-b961059d71bg", 10366, 309\.49 \]
\]

<a name='DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_'></a>

## TableConversionOptions\<UColumn\> Class

Provides options for converting a table to PostgreSQL format\.

```csharp
public class TableConversionOptions<UColumn>
    where UColumn : DiGi.Core.IO.Table.Interfaces.IColumn
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_.UColumn'></a>

`UColumn`

The type of column used in the table conversion, which must implement IColumn\.

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → TableConversionOptions\<UColumn\>
### Properties

<a name='DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_.IdentityColumn'></a>

## TableConversionOptions\<UColumn\>\.IdentityColumn Property

Gets or sets the column designated as the identity column\.

```csharp
public UColumn? IdentityColumn { get; set; }
```

#### Property Value
[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions\<UColumn\>\.UColumn')

<a name='DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_.PartitioningOptions'></a>

## TableConversionOptions\<UColumn\>\.PartitioningOptions Property

Gets or sets the partitioning options for the table\.

```csharp
public DiGi.PostgreSQL.Table.Classes.PartitioningOptions<UColumn>? PartitioningOptions { get; set; }
```

#### Property Value
[DiGi\.PostgreSQL\.Table\.Classes\.PartitioningOptions&lt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningOptions_UColumn_ 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningOptions\<UColumn\>')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions\<UColumn\>\.UColumn')[&gt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningOptions_UColumn_ 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningOptions\<UColumn\>')

<a name='DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_.PrimaryKeyColumns'></a>

## TableConversionOptions\<UColumn\>\.PrimaryKeyColumns Property

Gets or sets the list of columns that serve as the primary key for the table\.

```csharp
public System.Collections.Generic.List<UColumn>? PrimaryKeyColumns { get; set; }
```

#### Property Value
[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions\<UColumn\>\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')

<a name='DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_.UniqueColumns'></a>

## TableConversionOptions\<UColumn\>\.UniqueColumns Property

Gets or sets the list of columns that are defined with unique constraints\.

```csharp
public System.Collections.Generic.List<UColumn>? UniqueColumns { get; set; }
```

#### Property Value
[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions\<UColumn\>\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_'></a>

## TablePostgreSQLConverter\<UColumn\> Class

Provides an abstract base class for converters that handle the translation between a [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-1 'DiGi\.Core\.IO\.Table\.Classes\.Table\`1')
and its PostgreSQL representation\.

```csharp
public abstract class TablePostgreSQLConverter<UColumn> : DiGi.PostgreSQL.Classes.PostgreSQLConverter<DiGi.Core.IO.Table.Classes.Table<UColumn>>
    where UColumn : DiGi.Core.IO.Table.Interfaces.IColumn
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn'></a>

`UColumn`

The type of columns contained within the table, which must implement the [DiGi\.Core\.IO\.Table\.Interfaces\.IColumn](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.icolumn 'DiGi\.Core\.IO\.Table\.Interfaces\.IColumn') interface\.

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1')[DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-1 'DiGi\.Core\.IO\.Table\.Classes\.Table\`1')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-1 'DiGi\.Core\.IO\.Table\.Classes\.Table\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.postgresqlconverter-1 'DiGi\.PostgreSQL\.Classes\.PostgreSQLConverter\`1') → TablePostgreSQLConverter\<UColumn\>
### Constructors

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.TablePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData)'></a>

## TablePostgreSQLConverter\(ConnectionData\) Constructor

Initializes a new instance of the TablePostgreSQLConverter class using the specified connection data\.

```csharp
public TablePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData? connectionData);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.TablePostgreSQLConverter(DiGi.PostgreSQL.Classes.ConnectionData).connectionData'></a>

`connectionData` [DiGi\.PostgreSQL\.Classes\.ConnectionData](https://learn.microsoft.com/en-us/dotnet/api/digi.postgresql.classes.connectiondata 'DiGi\.PostgreSQL\.Classes\.ConnectionData')

The connection configuration details used to connect to the PostgreSQL database\. This value can be null\.
### Properties

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.TableConversionOptions'></a>

## TablePostgreSQLConverter\<UColumn\>\.TableConversionOptions Property

Gets the options used to configure the table conversion process\.

```csharp
protected abstract DiGi.PostgreSQL.Table.Classes.TableConversionOptions<UColumn>? TableConversionOptions { protected get; }
```

#### Property Value
[DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions&lt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_ 'DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions\<UColumn\>')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')[&gt;](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TableConversionOptions_UColumn_ 'DiGi\.PostgreSQL\.Table\.Classes\.TableConversionOptions\<UColumn\>')

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.TableName'></a>

## TablePostgreSQLConverter\<UColumn\>\.TableName Property

Gets the name of the database table associated with this entity\.

```csharp
public abstract string TableName { get; }
```

#### Property Value
[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')
### Methods

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.DetectSeparatorAsync(Npgsql.NpgsqlConnection,string,object)'></a>

## TablePostgreSQLConverter\<UColumn\>\.DetectSeparatorAsync\(NpgsqlConnection, string, object\) Method

Samples partition data to dynamically detect the most common separator \(comma, semicolon, or pipe\)\.

Resolves partitioning settings dynamically from [TableConversionOptions](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.TableConversionOptions 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.TableConversionOptions').

```csharp
public System.Threading.Tasks.Task<string> DetectSeparatorAsync(Npgsql.NpgsqlConnection npgsqlConnection, string columnUniqueId, object? partitionValue=null);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.DetectSeparatorAsync(Npgsql.NpgsqlConnection,string,object).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The active database connection instance\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.DetectSeparatorAsync(Npgsql.NpgsqlConnection,string,object).columnUniqueId'></a>

`columnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the column to sample\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.DetectSeparatorAsync(Npgsql.NpgsqlConnection,string,object).partitionValue'></a>

`partitionValue` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The partition key value; ignored if partitioning is disabled\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task representing the async operation, returning the detected separator character string \(e\.g\. ",", ";", or "\|"\)\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction,object,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetAggregateSummaryAsync\<TColumn\>\(NpgsqlConnection, string, MultivalueAggregateFunction, object, string, FilterGroup, int, CancellationToken\) Method

Computes multi\-value aggregate statistics on a specific column in a partition with optional dynamic filtering\.

Resolves partitioning settings dynamically from [TableConversionOptions](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.TableConversionOptions 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.TableConversionOptions').

```csharp
public System.Threading.Tasks.Task<System.Text.Json.Nodes.JsonNode?> GetAggregateSummaryAsync<TColumn>(Npgsql.NpgsqlConnection npgsqlConnection, string columnUniqueId, DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction multivalueAggregateFunction, object? partitionValue=null, string? separator=null, DiGi.PostgreSQL.Table.Classes.FilterGroup? filterGroup=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction,object,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The column type implementation\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction,object,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The active database connection instance\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction,object,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).columnUniqueId'></a>

`columnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the column to aggregate\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction,object,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).multivalueAggregateFunction'></a>

`multivalueAggregateFunction` [MultivalueAggregateFunction](DiGi.PostgreSQL.Table.Enums.md#DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction 'DiGi\.PostgreSQL\.Table\.Enums\.MultivalueAggregateFunction')

The multi\-value aggregation function to perform\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction,object,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).partitionValue'></a>

`partitionValue` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The partition key value; ignored if partitioning is disabled\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction,object,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).separator'></a>

`separator` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The custom separator character; if null, it is dynamically detected\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction,object,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).filterGroup'></a>

`filterGroup` [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')

The dynamic hierarchical filters to apply prior to aggregation\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction,object,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.MultivalueAggregateFunction,object,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Text\.Json\.Nodes\.JsonNode](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonnode 'System\.Text\.Json\.Nodes\.JsonNode')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task representing the async operation, returning the aggregation result as a [System\.Text\.Json\.Nodes\.JsonNode](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonnode 'System\.Text\.Json\.Nodes\.JsonNode')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetAggregateSummaryAsync\<TColumn\>\(NpgsqlConnection, string, SinglevalueAggregateFunction, object, FilterGroup, int, CancellationToken\) Method

Computes single\-value aggregate statistics on a specific column in a partition with optional dynamic filtering\.

Resolves partitioning settings dynamically from [TableConversionOptions](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.TableConversionOptions 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.TableConversionOptions').

```csharp
public System.Threading.Tasks.Task<System.Text.Json.Nodes.JsonNode?> GetAggregateSummaryAsync<TColumn>(Npgsql.NpgsqlConnection npgsqlConnection, string columnUniqueId, DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction singlevalueAggregateFunction, object? partitionValue=null, DiGi.PostgreSQL.Table.Classes.FilterGroup? filterGroup=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The column type implementation\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The active database connection instance\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).columnUniqueId'></a>

`columnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the column to aggregate\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).singlevalueAggregateFunction'></a>

`singlevalueAggregateFunction` [SinglevalueAggregateFunction](DiGi.PostgreSQL.Table.Enums.md#DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction 'DiGi\.PostgreSQL\.Table\.Enums\.SinglevalueAggregateFunction')

The single\-value aggregation function to perform\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).partitionValue'></a>

`partitionValue` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The partition key value; ignored if partitioning is disabled\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).filterGroup'></a>

`filterGroup` [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')

The dynamic hierarchical filters to apply prior to aggregation\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetAggregateSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Enums.SinglevalueAggregateFunction,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Text\.Json\.Nodes\.JsonNode](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonnode 'System\.Text\.Json\.Nodes\.JsonNode')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task representing the async operation, returning the aggregation result as a [System\.Text\.Json\.Nodes\.JsonNode](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonnode 'System\.Text\.Json\.Nodes\.JsonNode')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetCategoriesAsync(int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetCategoriesAsync\(int, CancellationToken\) Method

Asynchronously retrieves a unique set of categories from the database\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<string>?> GetCategoriesAsync(int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetCategoriesAsync(int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetCategoriesAsync(int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a [System\.Collections\.Generic\.HashSet&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1') of category strings if successful; otherwise, `null`\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetCategoriesAsync(Npgsql.NpgsqlConnection,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetCategoriesAsync\(NpgsqlConnection, int, CancellationToken\) Method

Asynchronously retrieves a unique set of categories from the database using the provided connection\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.HashSet<string>?> GetCategoriesAsync(Npgsql.NpgsqlConnection? npgsqlConnection, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetCategoriesAsync(Npgsql.NpgsqlConnection,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to execute the query\. This value can be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetCategoriesAsync(Npgsql.NpgsqlConnection,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetCategoriesAsync(Npgsql.NpgsqlConnection,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.HashSet&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a [System\.Collections\.Generic\.HashSet&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.hashset-1 'System\.Collections\.Generic\.HashSet\`1') of category strings if retrieved successfully; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(Npgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesAsync\(NpgsqlConnection, string, IEnumerable\<string\>, int, CancellationToken\) Method

Asynchronously reads the stored column metadata for this table, optionally narrowed to the rows whose [columnName](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(Npgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).columnName 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesAsync\(Npgsql\.NpgsqlConnection, string, System\.Collections\.Generic\.IEnumerable\<string\>, int, System\.Threading\.CancellationToken\)\.columnName') matches one of [values](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(Npgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).values 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesAsync\(Npgsql\.NpgsqlConnection, string, System\.Collections\.Generic\.IEnumerable\<string\>, int, System\.Threading\.CancellationToken\)\.values')\.

```csharp
private System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.ColumnReference>?> GetColumnReferencesAsync(Npgsql.NpgsqlConnection? npgsqlConnection, string columnName, System.Collections.Generic.IEnumerable<string>? values=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(Npgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to execute the query\. This value can be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(Npgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).columnName'></a>

`columnName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The metadata column to match on\. Callers pass a literal \- it is written into the statement and is never caller supplied\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(Npgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).values'></a>

`values` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The optional values to match\. Null or empty reads every column of the table\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(Npgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(Npgsql.NpgsqlConnection,string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the matching [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') list, or null when the connection is null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesAsync\(string, IEnumerable\<string\>, int, CancellationToken\) Method

Asynchronously opens a connection and reads the stored column metadata for this table, optionally narrowed to the rows whose [columnName](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).columnName 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesAsync\(string, System\.Collections\.Generic\.IEnumerable\<string\>, int, System\.Threading\.CancellationToken\)\.columnName') matches one of [values](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).values 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesAsync\(string, System\.Collections\.Generic\.IEnumerable\<string\>, int, System\.Threading\.CancellationToken\)\.values')\.

```csharp
private System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.ColumnReference>?> GetColumnReferencesAsync(string columnName, System.Collections.Generic.IEnumerable<string>? values=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).columnName'></a>

`columnName` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The metadata column to match on\. Callers pass a literal \- it is written into the statement and is never caller supplied\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).values'></a>

`values` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

The optional values to match\. Null or empty reads every column of the table\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesAsync(string,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains the matching [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') list, or null when no connection could be built\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByCategoriesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesByCategoriesAsync\(NpgsqlConnection, IEnumerable\<string\>, int, CancellationToken\) Method

Asynchronously retrieves a list of column references filtered by the specified categories\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.ColumnReference>?> GetColumnReferencesByCategoriesAsync(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<string>? categories=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByCategoriesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to be used for the database operation\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByCategoriesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).categories'></a>

`categories` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of category names used to filter the column references\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByCategoriesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByCategoriesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') objects if successful; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByCategoriesAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesByCategoriesAsync\(IEnumerable\<string\>, int, CancellationToken\) Method

Asynchronously retrieves a list of column references filtered by the specified categories\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.ColumnReference>?> GetColumnReferencesByCategoriesAsync(System.Collections.Generic.IEnumerable<string>? categories=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByCategoriesAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).categories'></a>

`categories` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of category names to filter the results\. If null, the filtering criteria may be omitted\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByCategoriesAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByCategoriesAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') objects if matches are found; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByNamesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesByNamesAsync\(NpgsqlConnection, IEnumerable\<string\>\) Method

Asynchronously retrieves a list of column references based on the specified names\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.ColumnReference>?> GetColumnReferencesByNamesAsync(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<string>? names=null);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByNamesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to execute the database query\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByNamesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_).names'></a>

`names` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of column names to filter the search results\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') objects if matches are found; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByNamesAsync(System.Collections.Generic.IEnumerable_string_)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesByNamesAsync\(IEnumerable\<string\>\) Method

Asynchronously retrieves a list of column references that match the specified names\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.ColumnReference>?> GetColumnReferencesByNamesAsync(System.Collections.Generic.IEnumerable<string>? names=null);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByNamesAsync(System.Collections.Generic.IEnumerable_string_).names'></a>

`names` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of column names to filter by\. If null, the retrieval criteria may vary based on the underlying implementation\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') objects if matches are found; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByUniqueIdsAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesByUniqueIdsAsync\(NpgsqlConnection, IEnumerable\<string\>\) Method

Asynchronously retrieves a list of column references associated with the specified unique identifiers\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.ColumnReference>?> GetColumnReferencesByUniqueIdsAsync(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<string>? columnUniqueIds=null);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByUniqueIdsAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to communicate with the database\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByUniqueIdsAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_).columnUniqueIds'></a>

`columnUniqueIds` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of unique identifier strings used to filter the column references\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') objects if matches are found; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByUniqueIdsAsync(System.Collections.Generic.IEnumerable_string_)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnReferencesByUniqueIdsAsync\(IEnumerable\<string\>\) Method

Asynchronously retrieves a list of column references associated with the specified unique identifiers\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<DiGi.PostgreSQL.Table.Classes.ColumnReference>?> GetColumnReferencesByUniqueIdsAsync(System.Collections.Generic.IEnumerable<string>? columnUniqueIds=null);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnReferencesByUniqueIdsAsync(System.Collections.Generic.IEnumerable_string_).columnUniqueIds'></a>

`columnUniqueIds` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of unique identifiers used to filter the column references\. If null, the retrieval behavior is determined by the underlying data source\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [ColumnReference](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ColumnReference 'DiGi\.PostgreSQL\.Table\.Classes\.ColumnReference') objects if matches are found; otherwise, [null](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/null 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/keywords/null')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsAsync()'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnsAsync\(\) Method

Asynchronously retrieves a list of all available column definitions\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<UColumn>?> GetColumnsAsync();
```

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn') objects if columns are found; otherwise, `null`\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByCategoriesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnsByCategoriesAsync\(NpgsqlConnection, IEnumerable\<string\>, int, CancellationToken\) Method

Asynchronously retrieves a list of columns filtered by the specified categories\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<UColumn>?> GetColumnsByCategoriesAsync(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<string>? categories=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByCategoriesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection to be used for the database operation\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByCategoriesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).categories'></a>

`categories` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of category names used to filter the retrieved columns\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByCategoriesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByCategoriesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn') objects if successful; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByCategoriesAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnsByCategoriesAsync\(IEnumerable\<string\>, int, CancellationToken\) Method

Asynchronously retrieves a list of columns filtered by the specified categories\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<UColumn>?> GetColumnsByCategoriesAsync(System.Collections.Generic.IEnumerable<string>? categories=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByCategoriesAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).categories'></a>

`categories` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of category names to filter the columns by\. If null, the filtering behavior is determined by the underlying data source\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByCategoriesAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByCategoriesAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn') objects matching the categories, or null if no results are found\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByNamesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnsByNamesAsync\(NpgsqlConnection, IEnumerable\<string\>\) Method

Asynchronously retrieves a list of columns filtered by the specified names\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<UColumn>?> GetColumnsByNamesAsync(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<string>? names=null);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByNamesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to execute the database query\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByNamesAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_).names'></a>

`names` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of column names to retrieve\. If null, the filter may be ignored or return no results depending on the underlying implementation\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn') objects if successful; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByNamesAsync(System.Collections.Generic.IEnumerable_string_)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnsByNamesAsync\(IEnumerable\<string\>\) Method

Asynchronously retrieves a list of columns filtered by the specified names\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<UColumn>?> GetColumnsByNamesAsync(System.Collections.Generic.IEnumerable<string>? names=null);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByNamesAsync(System.Collections.Generic.IEnumerable_string_).names'></a>

`names` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of column names to retrieve\. If null, the behavior depends on the underlying data source implementation\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn') objects if matches are found; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByUniqueIdsAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnsByUniqueIdsAsync\(NpgsqlConnection, IEnumerable\<string\>, int, CancellationToken\) Method

Asynchronously retrieves a list of columns based on their unique identifiers\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<UColumn>?> GetColumnsByUniqueIdsAsync(Npgsql.NpgsqlConnection? npgsqlConnection, System.Collections.Generic.IEnumerable<string>? columnUniqueIds=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByUniqueIdsAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to execute the database query\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByUniqueIdsAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).columnUniqueIds'></a>

`columnUniqueIds` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of unique identifier strings used to filter the results\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByUniqueIdsAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByUniqueIdsAsync(Npgsql.NpgsqlConnection,System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn') objects if found; otherwise, null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByUniqueIdsAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetColumnsByUniqueIdsAsync\(IEnumerable\<string\>, int, CancellationToken\) Method

Asynchronously retrieves a list of columns based on the provided unique identifiers\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.List<UColumn>?> GetColumnsByUniqueIdsAsync(System.Collections.Generic.IEnumerable<string>? columnUniqueIds=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByUniqueIdsAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).columnUniqueIds'></a>

`columnUniqueIds` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

An optional collection of unique identifier strings used to filter the columns\. If null, the behavior is determined by the underlying data source\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByUniqueIdsAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetColumnsByUniqueIdsAsync(System.Collections.Generic.IEnumerable_string_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.List&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1 'System\.Collections\.Generic\.List\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a list of [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn') objects matching the specified identifiers, or null if no matches are found\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetHistogramSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,int,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetHistogramSummaryAsync\<TColumn\>\(NpgsqlConnection, string, int, object, FilterGroup, int, CancellationToken\) Method

Generates a value distribution histogram for a specific column in a partition with optional dynamic filtering\.

Resolves partitioning settings dynamically from [TableConversionOptions](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.TableConversionOptions 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.TableConversionOptions').

```csharp
public System.Threading.Tasks.Task<System.Text.Json.Nodes.JsonArray?> GetHistogramSummaryAsync<TColumn>(Npgsql.NpgsqlConnection npgsqlConnection, string columnUniqueId, int bucketCount, object? partitionValue=null, DiGi.PostgreSQL.Table.Classes.FilterGroup? filterGroup=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetHistogramSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,int,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of column, which must implement [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetHistogramSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,int,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The active database connection instance\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetHistogramSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,int,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).columnUniqueId'></a>

`columnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the column to aggregate\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetHistogramSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,int,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).bucketCount'></a>

`bucketCount` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The total number of buckets to segment the value range into\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetHistogramSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,int,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).partitionValue'></a>

`partitionValue` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The partition key value; ignored if partitioning is disabled\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetHistogramSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,int,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).filterGroup'></a>

`filterGroup` [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')

The dynamic hierarchical filters to apply prior to aggregation\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetHistogramSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,int,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetHistogramSummaryAsync_TColumn_(Npgsql.NpgsqlConnection,string,int,object,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Text\.Json\.Nodes\.JsonArray](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonarray 'System\.Text\.Json\.Nodes\.JsonArray')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task representing the async operation, returning the histogram data as a [System\.Text\.Json\.Nodes\.JsonArray](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonarray 'System\.Text\.Json\.Nodes\.JsonArray')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetUniqueValuesAsync\<T\>\(NpgsqlConnection, string, FilterGroup, int, CancellationToken\) Method

Retrieves a distinct list of values from a specified column in the database\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.IEnumerable<T?>?> GetUniqueValuesAsync<T>(Npgsql.NpgsqlConnection? npgsqlConnection, string? columnUniqueId, DiGi.PostgreSQL.Table.Classes.FilterGroup? filterGroup=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).T'></a>

`T`

The target type for the retrieved values\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The active PostgreSQL connection instance\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).columnUniqueId'></a>

`columnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The name of the database column to query\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).filterGroup'></a>

`filterGroup` [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')

The optional hierarchical filters to apply prior to retrieving the unique values\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[T](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(Npgsql.NpgsqlConnection,string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).T 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.GetUniqueValuesAsync\<T\>\(Npgsql\.NpgsqlConnection, string, DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup, int, System\.Threading\.CancellationToken\)\.T')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
An enumerable containing unique values from the column, or null if input is invalid\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.GetUniqueValuesAsync\<T\>\(string, FilterGroup, int, CancellationToken\) Method

Asynchronously retrieves a collection of unique values associated with the specified identifier\.

```csharp
public System.Threading.Tasks.Task<System.Collections.Generic.IEnumerable<T?>?> GetUniqueValuesAsync<T>(string? columnUniqueId, DiGi.PostgreSQL.Table.Classes.FilterGroup? filterGroup=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken));
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).T'></a>

`T`

The type of the elements contained in the returned collection\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).columnUniqueId'></a>

`columnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the column used to query for the values; may be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).filterGroup'></a>

`filterGroup` [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')

The optional hierarchical filters to apply prior to retrieving the unique values\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[T](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).T 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.GetUniqueValuesAsync\<T\>\(string, DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup, int, System\.Threading\.CancellationToken\)\.T')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains an enumerable collection of nullable values of type [T](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.GetUniqueValuesAsync_T_(string,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,System.Threading.CancellationToken).T 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.GetUniqueValuesAsync\<T\>\(string, DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup, int, System\.Threading\.CancellationToken\)\.T'), or null if the operation cannot be completed or no data is found\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Table\<TColumn,TRow\>, FilterGroup, int, int, CancellationToken\) Method

Asynchronously pulls data from the database for the specified table using a filter group\.

```csharp
public System.Threading.Tasks.Task<bool> PullAsync<TColumn,TRow>(DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, DiGi.PostgreSQL.Table.Classes.FilterGroup filterGroup, int batchSize=1000, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of the column, which must derive from [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of the row, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup, int, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup, int, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table instance to pull data for\. This value can be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).filterGroup'></a>

`filterGroup` [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')

The filter group used to restrict the data retrieved from the database\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).batchSize'></a>

`batchSize` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The maximum number of rows to retrieve in each batch\. The default value is 1000\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool') if the data was pulled successfully; otherwise, [false](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Table\<TColumn,TRow\>, int, int, CancellationToken\) Method

Asynchronously pulls data from the database for the specified table using a defined batch size\.

```csharp
public System.Threading.Tasks.Task<bool> PullAsync<TColumn,TRow>(DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, int batchSize=1000, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of the column, which must derive from [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of the row, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, int, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, int, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table instance to pull data for\. This value can be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).batchSize'></a>

`batchSize` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The number of records to retrieve in each batch\. The default value is 1000\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool') if the data was pulled successfully; otherwise, [false](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Table\<TColumn,TRow\>, string, object, int, CancellationToken\) Method

Asynchronously pulls specific data from the specified table based on a unique column value\.

```csharp
public System.Threading.Tasks.Task<bool> PullAsync<TColumn,TRow>(DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, string columnUniqueId, object? value, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of the column, which must derive from [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of the row, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, object, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, object, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table instance from which data is being pulled\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).columnUniqueId'></a>

`columnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the column used to filter the data\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).value'></a>

`value` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The value used to identify the record to pull\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool') if the pull operation completed successfully; otherwise, [false](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(NpgsqlConnection, Table\<TColumn,TRow\>, FilterGroup, int, int, CancellationToken\) Method

Asynchronously pulls data from the specified table using the provided Npgsql connection, applying a filter group in batches\.

```csharp
public System.Threading.Tasks.Task<bool> PullAsync<TColumn,TRow>(Npgsql.NpgsqlConnection? npgsqlConnection, DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, DiGi.PostgreSQL.Table.Classes.FilterGroup filterGroup, int batchSize=1000, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of the column, which must derive from [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of the row, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql database connection to use for the operation\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup, int, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup, int, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table instance from which data is being pulled\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).filterGroup'></a>

`filterGroup` [FilterGroup](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.FilterGroup 'DiGi\.PostgreSQL\.Table\.Classes\.FilterGroup')

The filter group used to restrict the data retrieved from the database\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).batchSize'></a>

`batchSize` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The maximum number of rows to retrieve in each batch\. The default value is 1000\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,DiGi.PostgreSQL.Table.Classes.FilterGroup,int,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool') if the pull operation completed successfully; otherwise, [false](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(NpgsqlConnection, Table\<TColumn,TRow\>, int, int, CancellationToken\) Method

Asynchronously pulls data from the specified table using the provided Npgsql connection in batches\.

```csharp
public System.Threading.Tasks.Task<bool> PullAsync<TColumn,TRow>(Npgsql.NpgsqlConnection? npgsqlConnection, DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, int batchSize=1000, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of the column, which must derive from [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of the row, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql database connection to use for the operation\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, int, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, int, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table instance from which data is being pulled\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).batchSize'></a>

`batchSize` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The number of records to process per batch\. Defaults to 1000\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool') if the pull operation completed successfully; otherwise, [false](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(NpgsqlConnection, Table\<TColumn,TRow\>, string, object, int, object, int, CancellationToken\) Method

Asynchronously pulls a chunk of data from a table using keyset \(cursor\-based\) pagination\.

Resolves partitioning settings dynamically from [TableConversionOptions](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.TableConversionOptions 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.TableConversionOptions').

```csharp
public System.Threading.Tasks.Task<bool> PullAsync<TColumn,TRow>(Npgsql.NpgsqlConnection npgsqlConnection, DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, string seekColumnUniqueId, object? lastSeekValue, int pageSize, object? partitionValue=null, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of column, which must implement [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of row, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The active database connection instance\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, object, int, object, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, object, int, object, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table instance to populate with page data\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).seekColumnUniqueId'></a>

`seekColumnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the column to sort and seek by\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).lastSeekValue'></a>

`lastSeekValue` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The seek column value of the last row from the previous page\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).pageSize'></a>

`pageSize` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The maximum number of records to retrieve in this page\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).partitionValue'></a>

`partitionValue` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The partition key value; ignored if partitioning is disabled\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,object,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task representing the asynchronous operation, returning true if successful; otherwise, false\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(NpgsqlConnection, Table\<TColumn,TRow\>, string, object, int, CancellationToken\) Method

Asynchronously pulls specific data from the specified table based on a unique column value using the provided PostgreSQL connection\.

```csharp
public System.Threading.Tasks.Task<bool> PullAsync<TColumn,TRow>(Npgsql.NpgsqlConnection? npgsqlConnection, DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, string columnUniqueId, object? value, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of the column, which must derive from [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of the row, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection to be used for the operation\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, object, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, object, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table instance from which data is being pulled\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).columnUniqueId'></a>

`columnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the column used to filter the data\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).value'></a>

`value` [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object')

The value used to identify the record to pull\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,object,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool') if the pull operation completed successfully; otherwise, [false](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TObject,TColumn,TRow\>\(Table\<TColumn,TRow\>, string, IEnumerable\<TObject\>, int, CancellationToken\) Method

Asynchronously pulls specific data from the specified table based on unique column values\.

```csharp
public System.Threading.Tasks.Task<bool> PullAsync<TObject,TColumn,TRow>(DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, string columnUniqueId, System.Collections.Generic.IEnumerable<TObject>? values, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TObject'></a>

`TObject`

The type of the values used for filtering\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of the column, which must derive from [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of the row, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TObject,TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, System\.Collections\.Generic\.IEnumerable\<TObject\>, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TObject,TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, System\.Collections\.Generic\.IEnumerable\<TObject\>, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table instance from which data is being pulled\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).columnUniqueId'></a>

`columnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the column used to filter the data\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).values'></a>

`values` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[TObject](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TObject 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TObject,TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, System\.Collections\.Generic\.IEnumerable\<TObject\>, int, System\.Threading\.CancellationToken\)\.TObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of values used to identify the records to pull\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool') if the pull operation completed successfully; otherwise, [false](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TObject,TColumn,TRow\>\(NpgsqlConnection, Table\<TColumn,TRow\>, string, IEnumerable\<TObject\>, int, CancellationToken\) Method

Asynchronously pulls specific data from the specified table based on unique column values\.

```csharp
public System.Threading.Tasks.Task<bool> PullAsync<TObject,TColumn,TRow>(Npgsql.NpgsqlConnection? npgsqlConnection, DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, string columnUniqueId, System.Collections.Generic.IEnumerable<TObject>? values, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TObject'></a>

`TObject`

The type of the values being used for the pull operation\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of columns in the table, which must inherit from [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of rows in the table, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The PostgreSQL connection to be used for the operation\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TObject,TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, System\.Collections\.Generic\.IEnumerable\<TObject\>, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TObject,TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, System\.Collections\.Generic\.IEnumerable\<TObject\>, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table object from which data is being pulled\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).columnUniqueId'></a>

`columnUniqueId` [System\.String](https://learn.microsoft.com/en-us/dotnet/api/system.string 'System\.String')

The unique identifier of the column used to filter or identify the data\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).values'></a>

`values` [System\.Collections\.Generic\.IEnumerable&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')[TObject](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).TObject 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PullAsync\<TObject,TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, string, System\.Collections\.Generic\.IEnumerable\<TObject\>, int, System\.Threading\.CancellationToken\)\.TObject')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1 'System\.Collections\.Generic\.IEnumerable\`1')

A collection of values associated with the specified column unique ID\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds for the execution of the command\. A value of 0 disables the timeout\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PullAsync_TObject,TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,string,System.Collections.Generic.IEnumerable_TObject_,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains a [System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean') value indicating whether the pull operation was successful\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PushAsync\<TColumn,TRow\>\(Table\<TColumn,TRow\>, int, int, CancellationToken\) Method

Asynchronously pushes the contents of the specified table to the database using batch processing\.

When the converter is configured with primary key columns that are present on the table, the statement is an upsert - `ON CONFLICT (primary keys) DO UPDATE SET col = EXCLUDED.col` - and the update covers every non-primary-key column on the table, not only the cells that were set: a cell left unset on a row is written as NULL and overwrites the stored value of an existing row, while a column that is not on the table is never touched. Without such configuration the statement is a plain insert.

```csharp
public System.Threading.Tasks.Task<bool> PushAsync<TColumn,TRow>(DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, int batchSize=1000, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of the column, which must derive from [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of the row, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PushAsync\<TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, int, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PushAsync\<TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, int, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table instance containing the data to be pushed\. This value can be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).batchSize'></a>

`batchSize` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The number of records to process in each batch\. The default value is 1000\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds applied to every batch the push executes\. A value of 0 disables the timeout\. A batch carries [batchSize](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).batchSize 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PushAsync\<TColumn,TRow\>\(DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, int, int, System\.Threading\.CancellationToken\)\.batchSize') rows over every column of the table, so a wide table needs far more than the 30 second default\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\. Cancelling rolls the transaction back and throws rather than returning false\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result contains `true` if the push operation was successful; otherwise, `false`\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken)'></a>

## TablePostgreSQLConverter\<UColumn\>\.PushAsync\<TColumn,TRow\>\(NpgsqlConnection, Table\<TColumn,TRow\>, int, int, CancellationToken\) Method

Asynchronously pushes data from the specified table to the database using the provided Npgsql connection\.

When the converter is configured with primary key columns that are present on the table, the statement is an upsert - `ON CONFLICT (primary keys) DO UPDATE SET col = EXCLUDED.col` - and the update covers every non-primary-key column on the table, not only the cells that were set: a cell left unset on a row is written as NULL and overwrites the stored value of an existing row, while a column that is not on the table is never touched. Without such configuration the statement is a plain insert.

```csharp
public System.Threading.Tasks.Task<bool> PushAsync<TColumn,TRow>(Npgsql.NpgsqlConnection? npgsqlConnection, DiGi.Core.IO.Table.Classes.Table<TColumn,TRow>? table, int batchSize=1000, int commandTimeout=30, System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken))
    where TColumn : UColumn
    where TRow : DiGi.Core.IO.Table.Interfaces.IRow<TRow>;
```
#### Type parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TColumn'></a>

`TColumn`

The type of the column, which must derive from [UColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.UColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.UColumn')\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TRow'></a>

`TRow`

The type of the row, which must implement [DiGi\.Core\.IO\.Table\.Interfaces\.IRow&lt;&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.interfaces.irow-1 'DiGi\.Core\.IO\.Table\.Interfaces\.IRow\`1')\.
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).npgsqlConnection'></a>

`npgsqlConnection` [Npgsql\.NpgsqlConnection](https://learn.microsoft.com/en-us/dotnet/api/npgsql.npgsqlconnection 'Npgsql\.NpgsqlConnection')

The Npgsql connection instance used to communicate with the database\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).table'></a>

`table` [DiGi\.Core\.IO\.Table\.Classes\.Table&lt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TColumn](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TColumn 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PushAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, int, int, System\.Threading\.CancellationToken\)\.TColumn')[,](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')[TRow](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).TRow 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PushAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, int, int, System\.Threading\.CancellationToken\)\.TRow')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/digi.core.io.table.classes.table-2 'DiGi\.Core\.IO\.Table\.Classes\.Table\`2')

The table containing the data to be pushed\. May be null\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).batchSize'></a>

`batchSize` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The number of records to process per batch\. Defaults to 1000\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).commandTimeout'></a>

`commandTimeout` [System\.Int32](https://learn.microsoft.com/en-us/dotnet/api/system.int32 'System\.Int32')

The timeout in seconds applied to every batch the push executes\. A value of 0 disables the timeout\. A batch carries [batchSize](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).batchSize 'DiGi\.PostgreSQL\.Table\.Classes\.TablePostgreSQLConverter\<UColumn\>\.PushAsync\<TColumn,TRow\>\(Npgsql\.NpgsqlConnection, DiGi\.Core\.IO\.Table\.Classes\.Table\<TColumn,TRow\>, int, int, System\.Threading\.CancellationToken\)\.batchSize') rows over every column of the table, so a wide table needs far more than the 30 second default\.

<a name='DiGi.PostgreSQL.Table.Classes.TablePostgreSQLConverter_UColumn_.PushAsync_TColumn,TRow_(Npgsql.NpgsqlConnection,DiGi.Core.IO.Table.Classes.Table_TColumn,TRow_,int,int,System.Threading.CancellationToken).cancellationToken'></a>

`cancellationToken` [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken')

The [System\.Threading\.CancellationToken](https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken 'System\.Threading\.CancellationToken') to observe while waiting for the task to complete\. Cancelling rolls the transaction back and throws rather than returning false\.

#### Returns
[System\.Threading\.Tasks\.Task&lt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')[System\.Boolean](https://learn.microsoft.com/en-us/dotnet/api/system.boolean 'System\.Boolean')[&gt;](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.task-1 'System\.Threading\.Tasks\.Task\`1')  
A task that represents the asynchronous operation\. The task result is [true](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool') if the data was successfully pushed; otherwise, [false](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/bool 'https://docs\.microsoft\.com/en\-us/dotnet/csharp/language\-reference/builtin\-types/bool')\.

<a name='DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule'></a>

## ValuePartitioningRule Class

Represents a partitioning rule based on specific values\.

```csharp
public class ValuePartitioningRule : DiGi.PostgreSQL.Table.Classes.PartitioningRule
```

Inheritance [System\.Object](https://learn.microsoft.com/en-us/dotnet/api/system.object 'System\.Object') → [DiGi\.Core\.Classes\.Object](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.object 'DiGi\.Core\.Classes\.Object') → [DiGi\.Core\.Classes\.SerializableObject](https://learn.microsoft.com/en-us/dotnet/api/digi.core.classes.serializableobject 'DiGi\.Core\.Classes\.SerializableObject') → [PartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.PartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.PartitioningRule') → ValuePartitioningRule
### Constructors

<a name='DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule.ValuePartitioningRule()'></a>

## ValuePartitioningRule\(\) Constructor

Initializes a new instance of the [ValuePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.ValuePartitioningRule') class\.

```csharp
public ValuePartitioningRule();
```

<a name='DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule.ValuePartitioningRule(DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule)'></a>

## ValuePartitioningRule\(ValuePartitioningRule\) Constructor

Initializes a new instance of the [ValuePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.ValuePartitioningRule') class using an existing partitioning rule\.

```csharp
public ValuePartitioningRule(DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule valuePartitioningRule);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule.ValuePartitioningRule(DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule).valuePartitioningRule'></a>

`valuePartitioningRule` [ValuePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.ValuePartitioningRule')

The source partitioning rule to copy from\.

<a name='DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule.ValuePartitioningRule(System.Text.Json.Nodes.JsonObject)'></a>

## ValuePartitioningRule\(JsonObject\) Constructor

Initializes a new instance of the [ValuePartitioningRule](DiGi.PostgreSQL.Table.Classes.md#DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule 'DiGi\.PostgreSQL\.Table\.Classes\.ValuePartitioningRule') class using a JSON object\.

```csharp
public ValuePartitioningRule(System.Text.Json.Nodes.JsonObject jsonObject);
```
#### Parameters

<a name='DiGi.PostgreSQL.Table.Classes.ValuePartitioningRule.ValuePartitioningRule(System.Text.Json.Nodes.JsonObject).jsonObject'></a>

`jsonObject` [System\.Text\.Json\.Nodes\.JsonObject](https://learn.microsoft.com/en-us/dotnet/api/system.text.json.nodes.jsonobject 'System\.Text\.Json\.Nodes\.JsonObject')

The JSON object containing the rule definition\.