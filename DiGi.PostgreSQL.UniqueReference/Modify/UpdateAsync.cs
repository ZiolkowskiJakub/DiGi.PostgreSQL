using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.Enums;
using DiGi.PostgreSQL.UniqueReference.Classes;
using DiGi.PostgreSQL.UniqueReference.Delegates;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Modify
    {
        public static async Task<HashSet<Core.Classes.UniqueReference>?> UpdateAsync<USerializableObject>(this NpgsqlConnection? npgsqlConnection, IEnumerable<USerializableObject> serializableObjects, Func<Type?, DataType> dataTypeFunc, object? sender = null, UniqueIdReferenceGeneratingEventHandler? uniqueIdReferenceGeneratingEventHandler = null) where USerializableObject : ISerializableObject
        {
            if (npgsqlConnection is null || serializableObjects is null)
            {
                return null;
            }

            Dictionary<string, List<Tuple<Core.Classes.UniqueReference, USerializableObject>>> dictionary = [];
            foreach (USerializableObject serializableObject in serializableObjects)
            {
                if (serializableObject is null)
                {
                    continue;
                }

                UniqueIdReferenceGeneratingEventArgs uniqueIdReferenceGeneratingEventArgs = new(serializableObject);
                if (uniqueIdReferenceGeneratingEventHandler is not null && sender is not null)
                {
                    uniqueIdReferenceGeneratingEventHandler.Invoke(sender, uniqueIdReferenceGeneratingEventArgs);
                }

                Core.Classes.UniqueReference? uniqueReference = uniqueIdReferenceGeneratingEventArgs.Handled ? uniqueIdReferenceGeneratingEventArgs.UniqueIdReference : Core.Create.UniqueReference(serializableObject);
                if (uniqueReference?.TypeReference?.FullTypeName is not string fullTypeName)
                {
                    continue;
                }

                if (!dictionary.TryGetValue(fullTypeName, out List<Tuple<Core.Classes.UniqueReference, USerializableObject>>? tuples) || tuples is null)
                {
                    tuples = [];
                    dictionary[fullTypeName] = tuples;
                }

                tuples.Add(new Tuple<Core.Classes.UniqueReference, USerializableObject>(uniqueReference, serializableObject));
            }

            bool succeded;

            succeded = await Create.TableAsync_Partitions(npgsqlConnection);
            if (!succeded)
            {
                return null;
            }

            HashSet<Core.Classes.UniqueReference> result = [];

            if (dictionary.Count == 0)
            {
                return result;
            }

            await using NpgsqlBatch npgsqlBatch = new(npgsqlConnection);

            foreach (var keyValuePair in dictionary)
            {
                DataType dataType = dataTypeFunc.Invoke(Core.Query.Type(keyValuePair.Key));

                succeded = await Create.TableAsync_Objects(npgsqlConnection, dataType);
                if (!succeded)
                {
                    return null;
                }

                Partition? partition = await PostgreSQL.Modify.UpdatePartitionIdAsync(npgsqlConnection, keyValuePair.Key, dataType);
                if (partition is null)
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

                foreach (Tuple<Core.Classes.UniqueReference, USerializableObject> tuple in keyValuePair.Value)
                {
                    var uniqueReference = tuple.Item1;
                    var serializableObject = tuple.Item2;

                    object? value = PostgreSQL.Convert.ToPostgreSQL(serializableObject, dataType_Temp, out NpgsqlDbType npgsqlDbType);
                    if (npgsqlDbType == NpgsqlDbType.Unknown)
                    {
                        continue;
                    }

                    // Define the UPSERT command for this specific item
                    NpgsqlBatchCommand npgsqlBatchCommand = new($@"
                        INSERT INTO objects_{(int)dataType_Temp} (partition_id, unique_id, data)
                        VALUES (@partition_id, @unique_id, @data)
                        ON CONFLICT (partition_id, unique_id)
                        DO UPDATE SET data = EXCLUDED.data;");

                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("partition_id", NpgsqlDbType.Smallint) { Value = partition.Id });
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("unique_id", NpgsqlDbType.Text) { Value = uniqueReference.UniqueId });
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("data", npgsqlDbType) { Value = value });

                    npgsqlBatch.BatchCommands.Add(npgsqlBatchCommand);
                    result.Add(uniqueReference);
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