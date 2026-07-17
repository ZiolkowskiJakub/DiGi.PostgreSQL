using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.Enums;
using DiGi.PostgreSQL.PartitionReference.Classes;
using DiGi.PostgreSQL.PartitionReference.Delegates;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionReference
{
    public static partial class Modify
    {
        /// <summary>
        /// Asynchronously updates the specified serializable objects in the PostgreSQL database, utilizing partitions and UPSERT logic.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of the serializable object, which must implement <see cref="ISerializableObject"/>.</typeparam>
        /// <param name="npgsqlConnection">The Npgsql connection to be used for the database operation.</param>
        /// <param name="serializableObjects">The collection of objects to be updated.</param>
        /// <param name="dataTypeFunc">A function that determines the <see cref="DataType"/> based on the partition name.</param>
        /// <param name="sender">The object that sends the event, passed to the <paramref name="partitionReferenceGeneratingEventHandler"/>.</param>
        /// <param name="partitionReferenceGeneratingEventHandler">An optional event handler for generating partition references.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a HashSet of PartitionReference of updated partition references, or null if the connection or objects are null or if the table creation fails.</returns>
        public static async Task<HashSet<Classes.PartitionReference>?> UpdateAsync<USerializableObject>(this NpgsqlConnection? npgsqlConnection, IEnumerable<USerializableObject> serializableObjects, Func<string?, DataType> dataTypeFunc, object? sender = null, PartitionReferenceGeneratingEventHandler? partitionReferenceGeneratingEventHandler = null) where USerializableObject : ISerializableObject
        {
            if (npgsqlConnection is null || serializableObjects is null)
            {
                return null;
            }

            Dictionary<string, List<Tuple<Classes.PartitionReference, USerializableObject>>> dictionary = [];
            foreach (USerializableObject serializableObject in serializableObjects)
            {
                if (serializableObject is null)
                {
                    continue;
                }

                PartitionReferenceGeneratingEventArgs partitionReferenceGeneratingEventArgs = new(serializableObject);
                if (partitionReferenceGeneratingEventHandler is not null && sender is not null)
                {
                    partitionReferenceGeneratingEventHandler.Invoke(sender, partitionReferenceGeneratingEventArgs);
                }

                Classes.PartitionReference? partitionReference = null;
                if (partitionReferenceGeneratingEventArgs.Handled)
                {
                    partitionReference = partitionReferenceGeneratingEventArgs.PartitionReference;
                }
                else if (Core.Create.UniqueReference(serializableObject) is UniqueReference uniqueReference && uniqueReference?.TypeReference?.FullTypeName is string fullTypeName && uniqueReference.UniqueId is string uniqueId)
                {
                    partitionReference = new Classes.PartitionReference(fullTypeName, uniqueId);
                }

                if (partitionReference?.Name is not string name)
                {
                    continue;
                }

                if (!dictionary.TryGetValue(name, out List<Tuple<Classes.PartitionReference, USerializableObject>>? tuples) || tuples is null)
                {
                    tuples = [];
                    dictionary[name] = tuples;
                }

                tuples.Add(new Tuple<Classes.PartitionReference, USerializableObject>(partitionReference, serializableObject));
            }

            bool succeded;

            succeded = await PostgreSQL.Create.TableAsync_Partitions(npgsqlConnection);
            if (!succeded)
            {
                return null;
            }

            HashSet<Classes.PartitionReference> result = [];

            if (dictionary.Count == 0)
            {
                return result;
            }

            await using NpgsqlBatch npgsqlBatch = new(npgsqlConnection);

            foreach (var keyValuePair in dictionary)
            {
                DataType dataType = dataTypeFunc.Invoke(keyValuePair.Key);

                succeded = await PostgreSQL.Create.TableAsync_Objects(npgsqlConnection, dataType);
                if (!succeded)
                {
                    continue;
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

                foreach (Tuple<Classes.PartitionReference, USerializableObject> tuple in keyValuePair.Value)
                {
                    Classes.PartitionReference partitionReference = tuple.Item1;
                    USerializableObject serializableObject = tuple.Item2;

                    object? value = Convert.ToPostgreSQL(serializableObject, dataType_Temp, out NpgsqlDbType npgsqlDbType);
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
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("unique_id", NpgsqlDbType.Text) { Value = partitionReference.UniqueId });
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("data", npgsqlDbType) { Value = value });

                    npgsqlBatch.BatchCommands.Add(npgsqlBatchCommand);
                    result.Add(partitionReference);
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