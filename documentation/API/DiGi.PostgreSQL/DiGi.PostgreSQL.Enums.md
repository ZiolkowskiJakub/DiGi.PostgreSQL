#### [DiGi\.PostgreSQL](index.md 'index')

## DiGi\.PostgreSQL\.Enums Namespace
### Enums

<a name='DiGi.PostgreSQL.Enums.DataType'></a>

## DataType Enum

Defines the supported data types within the DiGi\.Core system\.

```csharp
public enum DataType
```
### Fields

<a name='DiGi.PostgreSQL.Enums.DataType.Undefined'></a>

`Undefined` 0

Undefined

<a name='DiGi.PostgreSQL.Enums.DataType.Json'></a>

`Json` 1

The JSON data type\.

<a name='DiGi.PostgreSQL.Enums.DataType.Binary'></a>

`Binary` 2

The binary data type\.

<a name='DiGi.PostgreSQL.Enums.DataType.Archive'></a>

`Archive` 3

The archive data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType'></a>

## PostgreSQLDataType Enum

Represents the PostgreSQL data types\.

```csharp
public enum PostgreSQLDataType
```
### Fields

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.Undefined'></a>

`Undefined` 0

Undefined data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.Integer'></a>

`Integer` 1

Integer data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.Bigint'></a>

`Bigint` 2

Bigint data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.Boolean'></a>

`Boolean` 3

Boolean data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.CharacterVarying'></a>

`CharacterVarying` 4

Character varying data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.Text'></a>

`Text` 5

Text data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.TimestampWithoutTimeZone'></a>

`TimestampWithoutTimeZone` 6

Timestamp without time zone data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.TimestampWithTimeZone'></a>

`TimestampWithTimeZone` 7

Timestamp with time zone data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.Numeric'></a>

`Numeric` 8

Numeric data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.Uuid'></a>

`Uuid` 9

Uuid data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.Jsonb'></a>

`Jsonb` 10

Jsonb data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.Bytea'></a>

`Bytea` 11

Bytea data type\.

<a name='DiGi.PostgreSQL.Enums.PostgreSQLDataType.Other'></a>

`Other` 12

Other data type\.

<a name='DiGi.PostgreSQL.Enums.StorageMethod'></a>

## StorageMethod Enum

Specifies the method used for data storage within the PostgreSQL database\.

```csharp
public enum StorageMethod
```
### Fields

<a name='DiGi.PostgreSQL.Enums.StorageMethod.Undefined'></a>

`Undefined` 0

Indicates that the storage method is undefined\.

<a name='DiGi.PostgreSQL.Enums.StorageMethod.UniqueReference'></a>

`UniqueReference` 1

Indicates storage using a unique reference identifier\.

<a name='DiGi.PostgreSQL.Enums.StorageMethod.Table'></a>

`Table` 2

Indicates storage using a standard table structure\.

<a name='DiGi.PostgreSQL.Enums.StorageMethod.PartitionReference'></a>

`PartitionReference` 3

Indicates storage using a partition reference\.

<a name='DiGi.PostgreSQL.Enums.StorageMethod.PartitionUniqueReference'></a>

`PartitionUniqueReference` 4

Indicates storage using a unique reference within a specific partition\.