using DiGi.Core;
using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
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
        public static async Task<HashSet<Classes.PartitionReference>?> UpdateAsync<USerializableObject>(this NpgsqlConnection? npgsqlConnection, IEnumerable<USerializableObject> serializableObjects, object? sender = null, PartitionReferenceGeneratingEventHandler? partitionReferenceGeneratingEventHandler = null) where USerializableObject : ISerializableObject
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

            succeded = await Create.Table_Partitions(npgsqlConnection);
            if (!succeded)
            {
                return null;
            }

            succeded = await Create.Table_Objects(npgsqlConnection);
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
                short? partitionId = await PostgreSQL.Modify.UpdateTypeIdAsync(npgsqlConnection, keyValuePair.Key);
                if (partitionId is null)
                {
                    continue;
                }

                foreach (Tuple<Classes.PartitionReference, USerializableObject> tuple in keyValuePair.Value)
                {
                    Classes.PartitionReference partitionReference = tuple.Item1;
                    USerializableObject serializableObject = tuple.Item2;

                    // Define the UPSERT command for this specific item
                    NpgsqlBatchCommand npgsqlBatchCommand = new(@"
                        INSERT INTO objects (partition_id, unique_id, data)
                        VALUES (@partition_id, @unique_id, @data)
                        ON CONFLICT (partition_id, unique_id)
                        DO UPDATE SET data = EXCLUDED.data;");

                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("partition_id", NpgsqlDbType.Smallint) { Value = partitionId.Value });
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("unique_id", NpgsqlDbType.Text) { Value = partitionReference.UniqueId });
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("data", NpgsqlDbType.Jsonb) { Value = serializableObject.ToSystem_String() });

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