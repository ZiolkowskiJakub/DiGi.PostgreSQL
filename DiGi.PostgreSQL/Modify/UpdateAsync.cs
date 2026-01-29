using DiGi.Core;
using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.Delegates;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL
{
    public static partial class Modify
    {
        public static async Task<HashSet<UniqueReference>?> UpdateAsync<USerializableObject>(this NpgsqlConnection? npgsqlConnection, IEnumerable<USerializableObject> serializableObjects, object? sender = null, UniqueReferenceGeneratingEventHandler? uniqueReferenceGeneratingEventHandler = null) where USerializableObject : ISerializableObject
        {
            if (npgsqlConnection is null || serializableObjects is null)
            {
                return null;
            }

            Dictionary<string, List<Tuple<UniqueReference, USerializableObject>>> dictionary = [];
            foreach (USerializableObject serializableObject in serializableObjects)
            {
                if (serializableObject is null)
                {
                    continue;
                }

                UniqueReferenceGeneratingEventArgs uniqueReferenceGeneratingEventArgs = new(serializableObject);
                if (uniqueReferenceGeneratingEventHandler is not null && sender is not null)
                {
                    uniqueReferenceGeneratingEventHandler.Invoke(sender, uniqueReferenceGeneratingEventArgs);
                }

                UniqueReference? uniqueReference = uniqueReferenceGeneratingEventArgs.Handled ? uniqueReferenceGeneratingEventArgs.UniqueReference : Core.Create.UniqueReference(serializableObject);
                if (uniqueReference?.TypeReference?.FullTypeName is not string fullTypeName)
                {
                    continue;
                }

                if (!dictionary.TryGetValue(fullTypeName, out List<Tuple<UniqueReference, USerializableObject>>? tuples) || tuples is null)
                {
                    tuples = [];
                    dictionary[fullTypeName] = tuples;
                }

                tuples.Add(new Tuple<UniqueReference, USerializableObject>(uniqueReference, serializableObject));
            }

            bool succeded;

            succeded = await Create.Table_Types(npgsqlConnection);
            if (!succeded)
            {
                return null;
            }

            succeded = await Create.Table_Objects(npgsqlConnection);
            if (!succeded)
            {
                return null;
            }

            HashSet<UniqueReference> result = [];

            if (dictionary.Count == 0)
            {
                return result;
            }

            await using NpgsqlBatch npgsqlBatch = new(npgsqlConnection);

            foreach (var keyValuePair in dictionary)
            {
                short? typeId = await Modify.UpdateTypeIdAsync(npgsqlConnection, keyValuePair.Key);
                if (typeId is null)
                {
                    continue;
                }

                foreach (var tuple in keyValuePair.Value)
                {
                    var uniqueReference = tuple.Item1;
                    var serializableObject = tuple.Item2;

                    // Define the UPSERT command for this specific item
                    NpgsqlBatchCommand npgsqlBatchCommand = new(@"
                        INSERT INTO objects (type_id, unique_id, data)
                        VALUES (@type_id, @unique_id, @data)
                        ON CONFLICT (type_id, unique_id)
                        DO UPDATE SET data = EXCLUDED.data;");

                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("type_id", NpgsqlDbType.Smallint) { Value = typeId.Value });
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("unique_id", NpgsqlDbType.Text) { Value = uniqueReference.UniqueId });
                    npgsqlBatchCommand.Parameters.Add(new NpgsqlParameter("data", NpgsqlDbType.Jsonb) { Value = serializableObject.ToSystem_String() });

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