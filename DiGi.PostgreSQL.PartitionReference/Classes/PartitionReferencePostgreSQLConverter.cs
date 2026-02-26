using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.Enums;
using DiGi.PostgreSQL.PartitionReference.Delegates;
using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionReference.Classes
{
    public class PartitionReferencePostgreSQLConverter<TSerializableObject> : PostgreSQLConverter<TSerializableObject> where TSerializableObject : ISerializableObject
    {
        public PartitionReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }

        public event PartitionReferenceGeneratingEventHandler? PartitionReferenceGenerating;

        public async Task<HashSet<PartitionReference>?> ContainsAsync<TUniqueReference>(IEnumerable<PartitionReference>? partitionReferences)
        {
            if (partitionReferences is null)
            {
                return null;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await Query.ContainsAsync(npgsqlConnection, partitionReferences);
        }

        public async Task<long> CountAsync(string name)
        {
            if (name is null)
            {
                return -1;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return -1;
            }

            npgsqlConnection.Open();

            short? partitionId = await PostgreSQL.Query.PartitionIdAsync(npgsqlConnection, name);
            if (partitionId is null)
            {
                return -1;
            }

            return await PostgreSQL.Query.CountAsync(npgsqlConnection, [partitionId.Value]);
        }

        public virtual DataType GetDataType(string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return DataType.Undefined;
            }

            return DataType.Json;
        }

        public async Task<USerializableObject?> GetSerializableObjectAsync<USerializableObject>(PartitionReference? partitionReference) where USerializableObject : TSerializableObject
        {
            if (partitionReference is null)
            {
                return default;
            }

            List<USerializableObject>? serializableObjects = await GetSerializableObjectsAsync<USerializableObject>([partitionReference]);
            if (serializableObjects is null || serializableObjects.Count == 0)
            {
                return default;
            }

            return serializableObjects[0];
        }

        public async Task<List<USerializableObject>?> GetSerializableObjectsAsync<USerializableObject>(IEnumerable<PartitionReference>? partitionReferences) where USerializableObject : TSerializableObject
        {
            if (partitionReferences is null)
            {
                return null;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await Query.SerializableObjectsAsync<USerializableObject>(npgsqlConnection, partitionReferences);
        }

        public async Task<List<USerializableObject>?> GetSerializableObjectsAsync<USerializableObject>(string? name) where USerializableObject : TSerializableObject
        {
            if (name is null)
            {
                return default;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return default;
            }

            npgsqlConnection.Open();

            return await Query.SerializableObjectsAsync<USerializableObject>(npgsqlConnection, name);
        }

        public async Task<PartitionReference?> RemoveAsync(PartitionReference? partitionReference)
        {
            if (partitionReference is null)
            {
                return default;
            }

            HashSet<PartitionReference>? partitionReferences = await RemoveAsync([partitionReference]);
            if (partitionReferences is null || partitionReferences.Count == 0)
            {
                return default;
            }

            return partitionReferences.First();
        }

        public async Task<HashSet<PartitionReference>?> RemoveAsync(IEnumerable<PartitionReference>? partitionReferences)
        {
            if (partitionReferences is null)
            {
                return null;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await Modify.RemoveAsync(npgsqlConnection, partitionReferences);
        }

        public async Task<bool> RemoveAsync(string? name)
        {
            if (name is null)
            {
                return false;
            }

            HashSet<string>? names = await RemoveAsync([name]);

            return names != null && names.Contains(name);
        }

        public async Task<HashSet<string>?> RemoveAsync(IEnumerable<string>? names)
        {
            if (names is null)
            {
                return null;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            HashSet<string> result = [];
            foreach (string name in names)
            {
                short? partitionId = await PostgreSQL.Query.PartitionIdAsync(npgsqlConnection, name);
                if (partitionId is null)
                {
                    continue;
                }

                bool removed = await PostgreSQL.Modify.RemoveAsync(npgsqlConnection, [partitionId.Value]);
                if (removed)
                {
                    result.Add(name);
                }
            }

            return result;
        }

        public async Task<PartitionReference?> UpdateAsync(TSerializableObject? serializableObject)
        {
            if (serializableObject is null)
            {
                return null;
            }

            HashSet<PartitionReference>? partitionReferences = await UpdateAsync([serializableObject]);
            if (partitionReferences is null || partitionReferences.Count == 0)
            {
                return null;
            }

            return partitionReferences.First();
        }

        public async Task<HashSet<PartitionReference>?> UpdateAsync<USerializableObject>(IEnumerable<USerializableObject>? serializableObjects) where USerializableObject : TSerializableObject
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

            return await Modify.UpdateAsync(npgsqlConnection, serializableObjects, GetDataType, this, PartitionReferenceGenerating);
        }
    }

    public class PartitionReferencePostgreSQLConverter : PartitionReferencePostgreSQLConverter<ISerializableObject>
    {
        public PartitionReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }
    }
}