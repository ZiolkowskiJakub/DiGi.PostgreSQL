using DiGi.Core;
using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Delegates;
using DiGi.PostgreSQL.Interfaces;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.Classes
{
    public abstract class PostgreSQLConverter<TSerializableObject> : IPostgreSQLObject where TSerializableObject : ISerializableObject
    {
        public PostgreSQLConverter(ConnectionData connectionData)
        {
            ConnectionData = connectionData;
        }

        public event UniqueReferenceGeneratingEventHandler? UniqueReferenceGenerating;

        public ConnectionData ConnectionData { get; set; }

        public async Task<List<USerializableObject>?> GetAsync<USerializableObject>(bool inheritance = true) where USerializableObject : TSerializableObject
        {
            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await GetAsync<USerializableObject>(npgsqlConnection, inheritance);
        }

        public async Task<USerializableObject?> GetAsync<USerializableObject>(IUniqueReference uniqueReference) where USerializableObject : TSerializableObject
        {
            if (uniqueReference is null)
            {
                return default;
            }

            List<USerializableObject>? serializableObjects = await GetAsync<USerializableObject, IUniqueReference>([uniqueReference]);
            if (serializableObjects is null || serializableObjects.Count == 0)
            {
                return default;
            }

            return serializableObjects[0];
        }

        public async Task<List<USerializableObject>?> GetAsync<USerializableObject, TUniqueReference>(IEnumerable<TUniqueReference> uniqueReferences) where USerializableObject : TSerializableObject where TUniqueReference : IUniqueReference
        {
            if (uniqueReferences is null)
            {
                return null;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await GetAsync<USerializableObject, TUniqueReference>(npgsqlConnection, uniqueReferences);
        }

        public async Task<TUniqueReference?> RemoveAsync<TUniqueReference>(TUniqueReference uniqueReference) where TUniqueReference : IUniqueReference
        {
            if (uniqueReference is null)
            {
                return default;
            }
            List<TUniqueReference>? uniqueReferences = await RemoveAsync([uniqueReference]);
            if (uniqueReferences is null || uniqueReferences.Count == 0)
            {
                return default;
            }

            return uniqueReferences[0];
        }

        public async Task<List<TUniqueReference>?> RemoveAsync<TUniqueReference>(IEnumerable<TUniqueReference> uniqueReferences) where TUniqueReference : IUniqueReference
        {
            if (uniqueReferences is null)
            {
                return null;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await RemoveAsync(npgsqlConnection, uniqueReferences);
        }

        public async Task<UniqueReference?> UpdateAsync(TSerializableObject serializableObjects)
        {
            if (serializableObjects is null)
            {
                return null;
            }

            List<UniqueReference>? uniqueReferences = await UpdateAsync([serializableObjects]);
            if (uniqueReferences is null || uniqueReferences.Count == 0)
            {
                return null;
            }

            return uniqueReferences[0];
        }

        public async Task<List<UniqueReference>?> UpdateAsync<USerializableObject>(IEnumerable<USerializableObject> serializableObjects) where USerializableObject : TSerializableObject
        {
            if (serializableObjects is null)
            {
                return null;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await UpdateAsync(npgsqlConnection, serializableObjects);
        }

        private static async Task<List<USerializableObject>?> GetAsync<USerializableObject>(NpgsqlConnection? npgsqlConnection, bool inheritance = true) where USerializableObject : TSerializableObject
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            Dictionary<short, Type>? dictionary = null;
            if (inheritance)
            {
                dictionary = await Query.TypeIds(npgsqlConnection, typeof(USerializableObject));
            }
            else
            {
                Type type = typeof(USerializableObject);
                short? typeId = await Query.TypeId(npgsqlConnection, type);
                if (typeId.HasValue)
                {
                    dictionary = new Dictionary<short, Type>()
                    {
                        { typeId.Value, type }
                    };
                }
            }

            if (dictionary is null)
            {
                return null;
            }

            List<USerializableObject> result = [];
            if (dictionary.Count == 0)
            {
                return result;
            }

            string commandText = @"
                SELECT data 
                FROM objects 
                WHERE type_id = ANY(@type_ids);";

            await using var npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);

            npgsqlCommand.Parameters.AddWithValue("type_ids", dictionary.Keys.ToArray());

            await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

            while (await npgsqlDataReader.ReadAsync())
            {
                string data = npgsqlDataReader.GetString(0);

                if (Core.Convert.ToDiGi<USerializableObject>(data) is not List<USerializableObject> serializableObjects || serializableObjects.Count == 0)
                {
                    continue;
                }

                if (serializableObjects[0] is not USerializableObject serializableObject)
                {
                    continue;
                }

                result.Add(serializableObject);
            }

            return result;
        }

        private static async Task<List<USerializableObject>?> GetAsync<USerializableObject, TUniqueReference>(NpgsqlConnection? npgsqlConnection, IEnumerable<TUniqueReference> uniqueReferences) where USerializableObject : TSerializableObject where TUniqueReference : IUniqueReference
        {
            if (npgsqlConnection is null || uniqueReferences is null)
            {
                return null;
            }

            Dictionary<string, List<string>> dictionary = [];
            foreach (TUniqueReference uniqueReference in uniqueReferences)
            {
                if (uniqueReference?.TypeReference?.FullTypeName is not string fullTypeName || uniqueReference.UniqueId is not string uniqueId)
                {
                    continue;
                }

                if (!dictionary.TryGetValue(fullTypeName, out List<string>? uniqueIds) || uniqueIds is null)
                {
                    uniqueIds = [];
                    dictionary[fullTypeName] = uniqueIds;
                }

                uniqueIds.Add(uniqueId);
            }

            string commandText = @"
                SELECT o.data
                FROM objects o
                JOIN (
                    SELECT UNNEST(@type_ids) as t_id, UNNEST(@unique_ids) as u_id
                ) as search_set ON o.type_id = search_set.t_id AND o.unique_id = search_set.u_id;";

            await using var npgsqlCommand = new NpgsqlCommand(commandText, npgsqlConnection);

            npgsqlCommand.Parameters.Add("type_ids", NpgsqlDbType.Array | NpgsqlDbType.Smallint);
            npgsqlCommand.Parameters.Add("unique_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);

            List<USerializableObject> result = [];
            foreach (KeyValuePair<string, List<string>> keyValuePair in dictionary)
            {
                Dictionary<short, Type>? dictionary_Types = await Query.TypeIds(npgsqlConnection, keyValuePair.Key);
                if (dictionary_Types is null || dictionary_Types.Count == 0)
                {
                    continue;
                }

                npgsqlCommand.Parameters["type_ids"].Value = dictionary_Types.Keys.ToArray();
                npgsqlCommand.Parameters["unique_ids"].Value = keyValuePair.Value.ToArray();

                await using NpgsqlDataReader npgsqlDataReader = await npgsqlCommand.ExecuteReaderAsync();

                while (await npgsqlDataReader.ReadAsync())
                {
                    string data = npgsqlDataReader.GetString(0);

                    if (Core.Convert.ToDiGi<USerializableObject>(data) is not List<USerializableObject> serializableObjects || serializableObjects.Count == 0)
                    {
                        continue;
                    }

                    if (serializableObjects[0] is not USerializableObject serializableObject)
                    {
                        continue;
                    }

                    result.Add(serializableObject);
                }
            }

            return result;
        }

        private static async Task<List<TUniqueReference>?> RemoveAsync<TUniqueReference>(NpgsqlConnection? npgsqlConnection, IEnumerable<TUniqueReference> uniqueReferences) where TUniqueReference : IUniqueReference
        {
            if (npgsqlConnection is null)
            {
                return null;
            }

            // Use RETURNING o.type_id so we know exactly which partition was affected
            string commandText_Delete = @"
                DELETE FROM objects o
                USING types t
                WHERE o.type_id = t.id
                  AND t.full_name = @full_name
                  AND o.unique_id = @unique_id
                RETURNING o.type_id;";

            List<TUniqueReference> result = [];
            HashSet<short> typeIds = [];

            await using NpgsqlCommand npgsqlCommand = new(commandText_Delete, npgsqlConnection);
            npgsqlCommand.Parameters.Add("full_name", NpgsqlDbType.Text);
            npgsqlCommand.Parameters.Add("unique_id", NpgsqlDbType.Text);

            foreach (TUniqueReference uniqueReference in uniqueReferences)
            {
                if (uniqueReference?.TypeReference?.FullTypeName is not string fullTypeName || string.IsNullOrWhiteSpace(uniqueReference.UniqueId))
                {
                    continue;
                }

                npgsqlCommand.Parameters["full_name"].Value = fullTypeName;
                npgsqlCommand.Parameters["unique_id"].Value = uniqueReference.UniqueId;

                if (await npgsqlCommand.ExecuteScalarAsync() is short typeId)
                {
                    result.Add(uniqueReference);
                    typeIds.Add(typeId); // Track this type for cleanup
                }
            }

            // --- CLEANUP SECTION ---
            foreach (short typeId in typeIds)
            {
                // Check if the partition is now empty
                await using NpgsqlCommand npgsqlCommand_Check = new("SELECT NOT EXISTS(SELECT 1 FROM objects WHERE type_id = @type_id);", npgsqlConnection);
                npgsqlCommand_Check.Parameters.Add("type_id", NpgsqlDbType.Smallint).Value = typeId;

                bool isEmpty = (bool)(await npgsqlCommand_Check.ExecuteScalarAsync() ?? false);
                if (isEmpty)
                {
                    // 1. Remove from types first (Metadata)
                    await using NpgsqlCommand npgsqlCommand_DeleteType = new("DELETE FROM types WHERE id = @type_id;", npgsqlConnection);
                    npgsqlCommand_DeleteType.Parameters.Add("type_id", NpgsqlDbType.Smallint).Value = typeId;
                    await npgsqlCommand_DeleteType.ExecuteNonQueryAsync();

                    // 2. Drop the physical partition table
                    await using NpgsqlCommand npgsqlCommand_DropTable = new($"DROP TABLE IF EXISTS objects_type_{typeId};", npgsqlConnection);
                    await npgsqlCommand_DropTable.ExecuteNonQueryAsync();
                }
            }

            return result;
        }

        private async Task<List<UniqueReference>?> UpdateAsync<USerializableObject>(NpgsqlConnection? npgsqlConnection, IEnumerable<USerializableObject> serializableObjects) where USerializableObject : TSerializableObject
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
                UniqueReferenceGenerating?.Invoke(this, uniqueReferenceGeneratingEventArgs);

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

            List<UniqueReference> result = [];

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


    public class PostgreSQLConverter : PostgreSQLConverter<ISerializableObject>
    {
        public PostgreSQLConverter(ConnectionData connectionData)
            : base(connectionData)
        {

        }
    }
}
