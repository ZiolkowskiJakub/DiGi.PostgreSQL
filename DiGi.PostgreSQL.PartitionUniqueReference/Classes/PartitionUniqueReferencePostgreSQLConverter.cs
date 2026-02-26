using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.Enums;
using DiGi.PostgreSQL.PartitionUniqueReference.Delegates;
using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.PartitionUniqueReference.Classes
{
    public class PartitionUniqueReferencePostgreSQLConverter<TSerializableObject> : PostgreSQLConverter<TSerializableObject> where TSerializableObject : ISerializableObject
    {
        public PartitionUniqueReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }

        public event PartitionUniqueReferenceGeneratingEventHandler? PartitionUniqueReferenceReferenceGenerating;

        public async Task<bool> Clean(bool partitions = true, bool types = true)
        {
            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            npgsqlConnection.Open();

            List<Partition>? partitions_Result = null;
            
            if(partitions)
            {
                partitions_Result = await PostgreSQL.Modify.CleanPartitionsAsync(npgsqlConnection);
            }

            List<Type>? types_Result = null;
            if(types)
            {
                types_Result = await Modify.CleanTypesAsync(npgsqlConnection);
            }

            return (partitions_Result != null && partitions_Result.Count != 0) || (types_Result != null && types_Result.Count != 0);
        }

        public async Task<bool> ContainsAsync(Type? type)
        {
            if (type is null)
            {
                return false;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            npgsqlConnection.Open();

            return await Query.ContainsAsync(npgsqlConnection, type);
        }

        public async Task<bool> ContainsAsync(System.Type? type)
        {
            if (type is null)
            {
                return false;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            npgsqlConnection.Open();

            return await Query.ContainsAsync(npgsqlConnection, type);
        }

        public async Task<HashSet<PartitionUniqueReference>?> ContainsAsync(IEnumerable<PartitionUniqueReference>? partitionUniqueReferences)
        {
            if (partitionUniqueReferences is null)
            {
                return null;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await Query.ContainsAsync(npgsqlConnection, partitionUniqueReferences);
        }

        public virtual DataType GetDataType(string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return DataType.Undefined;
            }

            return DataType.Json;
        }
        
        public async Task<USerializableObject?> GetSerializableObjectAsync<USerializableObject>(PartitionUniqueReference? partitionUniqueReference) where USerializableObject : TSerializableObject
        {
            if (partitionUniqueReference is null)
            {
                return default;
            }

            List<USerializableObject>? serializableObjects = await GetSerializableObjectsAsync<USerializableObject>([partitionUniqueReference]);
            if (serializableObjects is null || serializableObjects.Count == 0)
            {
                return default;
            }

            return serializableObjects[0];
        }

        public async Task<List<USerializableObject>?> GetSerializableObjectsAsync<USerializableObject>(IEnumerable<PartitionUniqueReference>? partitionUniqueReferences) where USerializableObject : TSerializableObject
        {
            if (partitionUniqueReferences is null)
            {
                return null;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await Query.SerializableObjectsAsync<USerializableObject>(npgsqlConnection, partitionUniqueReferences);
        }

        public async Task<HashSet<PartitionUniqueReference>?> RemoveAsync(IEnumerable<PartitionUniqueReference>? partitionUniqueReferences, bool clean = true)
        {
            if (partitionUniqueReferences is null)
            {
                return null;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await Modify.RemoveAsync(npgsqlConnection, partitionUniqueReferences, clean);
        }

        public async Task<PartitionUniqueReference?> RemoveAsync(PartitionUniqueReference? partitionUniqueReference, bool clean = true)
        {
            if(partitionUniqueReference is null)
            {
                return null;
            }

            HashSet<PartitionUniqueReference>? partitionUniqueReferences = await RemoveAsync([partitionUniqueReference], clean);
            if(partitionUniqueReferences is null || partitionUniqueReferences.Count == 0)
            {
                return null; 
            }

            return partitionUniqueReferences.First();
        }

        public async Task<PartitionUniqueReference?> UpdateAsync(TSerializableObject? serializableObject)
        {
            if (serializableObject is null)
            {
                return null;
            }

            HashSet<PartitionUniqueReference>? uniqueReferences = await UpdateAsync([serializableObject]);
            if (uniqueReferences is null || uniqueReferences.Count == 0)
            {
                return null;
            }

            return uniqueReferences.First();
        }

        public async Task<HashSet<PartitionUniqueReference>?> UpdateAsync<USerializableObject>(IEnumerable<USerializableObject>? serializableObjects) where USerializableObject : TSerializableObject
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

            return await Modify.UpdateAsync(npgsqlConnection, serializableObjects, GetDataType, this, PartitionUniqueReferenceReferenceGenerating);
        }
    }

    public class PartitionUniqueReferencePostgreSQLConverter : PartitionUniqueReferencePostgreSQLConverter<ISerializableObject>
    {
        public PartitionUniqueReferencePostgreSQLConverter(ConnectionData? connectionData) 
            : base(connectionData)
        {
        }
    }
}