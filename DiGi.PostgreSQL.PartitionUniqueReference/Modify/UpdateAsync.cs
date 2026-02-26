using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.Enums;
using DiGi.PostgreSQL.PartitionUniqueReference.Classes;
using DiGi.PostgreSQL.PartitionUniqueReference.Delegates;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Modify
    {
        public static async Task<HashSet<Classes.PartitionUniqueReference>?> UpdateAsync<USerializableObject>(this NpgsqlConnection? npgsqlConnection, IEnumerable<USerializableObject> serializableObjects, Func<string?, DataType> dataTypeFunc, object? sender = null, PartitionUniqueReferenceGeneratingEventHandler? partitionUniqueReferenceGeneratingEventHandler = null) where USerializableObject : ISerializableObject
        {
            if (npgsqlConnection is null || serializableObjects is null)
            {
                return null;
            }

            Dictionary<string, List<Tuple<Classes.PartitionUniqueReference, USerializableObject>>> dictionary = [];
            foreach (USerializableObject serializableObject in serializableObjects)
            {
                if (serializableObject is null)
                {
                    continue;
                }

                PartitionUniqueReferenceGeneratingEventArgs partitionUniqueReferenceGeneratingEventArgs = new(serializableObject);
                if (partitionUniqueReferenceGeneratingEventHandler is not null && sender is not null)
                {
                    partitionUniqueReferenceGeneratingEventHandler.Invoke(sender, partitionUniqueReferenceGeneratingEventArgs);
                }

                Classes.PartitionUniqueReference? partitionUniqueReference = null;
                if (partitionUniqueReferenceGeneratingEventArgs.Handled)
                {
                    partitionUniqueReference = partitionUniqueReferenceGeneratingEventArgs.PartitionUniqueReference;
                }
                else if (Core.Create.UniqueReference(serializableObject) is Core.Classes.UniqueReference uniqueReference && uniqueReference?.TypeReference?.FullTypeName is string fullTypeName)
                {
                    partitionUniqueReference = new Classes.PartitionUniqueReference(fullTypeName, uniqueReference);
                }

                if (partitionUniqueReference?.Name is not string name)
                {
                    continue;
                }

                if (!dictionary.TryGetValue(name, out List<Tuple<Classes.PartitionUniqueReference, USerializableObject>>? tuples) || tuples is null)
                {
                    tuples = [];
                    dictionary[name] = tuples;
                }

                tuples.Add(new Tuple<Classes.PartitionUniqueReference, USerializableObject>(partitionUniqueReference, serializableObject));
            }

            bool succeded;

            succeded = await Create.TableAsync_Partitions(npgsqlConnection);
            if (!succeded)
            {
                return null;
            }

            succeded = await Create.TableAsync_Types(npgsqlConnection);
            if (!succeded)
            {
                return null;
            }

            HashSet<Classes.PartitionUniqueReference> result = [];

            if (dictionary.Count == 0)
            {
                return result;
            }

            await using NpgsqlBatch npgsqlBatch = new(npgsqlConnection);

            foreach (KeyValuePair<string, List<Tuple<Classes.PartitionUniqueReference, USerializableObject>>> keyValuePair in dictionary)
            {
                DataType dataType = dataTypeFunc.Invoke(keyValuePair.Key);

                succeded = await Create.TableAsync_Objects(npgsqlConnection, dataType, false, true);
                if (!succeded)
                {
                    continue;
                }

                Partition? partition = await PostgreSQL.Modify.UpdatePartitionIdAsync(npgsqlConnection, keyValuePair.Key, dataType);
                if (partition is null)
                {
                    continue;
                }

                List<Tuple<Classes.PartitionUniqueReference, USerializableObject>>? tuples = keyValuePair.Value;

                while (tuples is not null && tuples.Count > 0)
                {
                    System.Type type_Temp = tuples[0].Item2.GetType();

                    Core.Query.Filter(tuples, x => type_Temp == x!.Item2.GetType(), out List<Tuple<Classes.PartitionUniqueReference, USerializableObject>>? tuples_In, out List<Tuple<Classes.PartitionUniqueReference, USerializableObject>>? tuples_Out);
                    if (tuples_In is null || tuples_In.Count == 0)
                    {
                        break;
                    }

                    tuples = tuples_Out;

                    Classes.Type? type = await UpdateTypeIdAsync(npgsqlConnection, Core.Query.FullTypeName(type_Temp));
                    if (type is null)
                    {
                        continue;
                    }

                    DataType dataType_Temp = partition.DataType;
                    if (dataType_Temp == DataType.Undefined)
                    {
                        dataType_Temp = dataType;
                    }

                    if (dataType_Temp == DataType.Undefined)
                    {
                        continue;
                    }

                    foreach (Tuple<Classes.PartitionUniqueReference, USerializableObject> tuple in keyValuePair.Value)
                    {
                        Classes.PartitionUniqueReference partitionUniqueReference = tuple.Item1;

                        if (partitionUniqueReference.UniqueReference?.UniqueId is not string uniqueId || string.IsNullOrWhiteSpace(uniqueId))
                        {
                            continue;
                        }

                        USerializableObject serializableObject = tuple.Item2;

                        object? value = Convert.ToPostgreSQL(serializableObject, dataType_Temp, out NpgsqlDbType npgsqlDbType);
                        if (npgsqlDbType == NpgsqlDbType.Unknown)
                        {
                            continue;
                        }

                        // Define the UPSERT command for this specific item
                        NpgsqlBatchCommand npgsqlBatchCommand = new($@"
                        INSERT INTO objects_{(int)dataType_Temp} (partition_id, type_id, unique_id, data)
                        VALUES (@partition_id, @type_id, @unique_id, @data)
                        ON CONFLICT (partition_id, type_id, unique_id)
                        DO UPDATE SET data = EXCLUDED.data;");

                        npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("partition_id", NpgsqlDbType.Smallint) { Value = partition.Id });
                        npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("type_id", NpgsqlDbType.Smallint) { Value = type.Id });
                        npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("unique_id", NpgsqlDbType.Text) { Value = uniqueId });
                        npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("data", npgsqlDbType) { Value = value });

                        npgsqlBatch.BatchCommands.Add(npgsqlBatchCommand);
                        result.Add(partitionUniqueReference);
                    }
                }
            }

            if (npgsqlBatch.BatchCommands.Count > 0)
            {
                await npgsqlBatch.ExecuteNonQueryAsync();
            }

            return result;
        }
    }
}