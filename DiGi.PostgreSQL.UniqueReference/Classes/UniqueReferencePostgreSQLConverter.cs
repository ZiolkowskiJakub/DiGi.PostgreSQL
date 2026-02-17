using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.Enums;
using DiGi.PostgreSQL.UniqueReference.Delegates;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference.Classes
{
    public class UniqueReferencePostgreSQLConverter<TSerializableObject> : PostgreSQLConverter<TSerializableObject> where TSerializableObject : ISerializableObject
    {
        public UniqueReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }

        public event UniqueIdReferenceGeneratingEventHandler? UniqueIdReferenceGenerating;

        public async Task<bool> Contains(Type? type)
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

            return await Query.Contains(npgsqlConnection, type);
        }

        public async Task<HashSet<TUniqueReference>?> Contains<TUniqueReference>(IEnumerable<TUniqueReference>? uniqueReferences) where TUniqueReference : IUniqueReference
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

            return await Query.Contains(npgsqlConnection, uniqueReferences);
        }

        public async Task<long> CountAsync(Type? type, bool inheritance = true)
        {
            if (type is null)
            {
                return -1;
            }

            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return -1;
            }

            npgsqlConnection.Open();

            return await Query.CountAsync(npgsqlConnection, type, inheritance);
        }

        public async Task<long> CountAsync<USerializableObject>(bool inheritance = true) where USerializableObject : TSerializableObject
        {
            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return -1;
            }
            npgsqlConnection.Open();

            return await Query.CountAsync(npgsqlConnection, typeof(USerializableObject), inheritance);
        }

        public virtual DataType GetDataType(Type? type)
        {
            if (type is null)
            {
                return DataType.Undefined;
            }

            if (!typeof(TSerializableObject).IsAssignableFrom(type))
            {
                return DataType.Undefined;
            }

            return DataType.Json;
        }

        public DataType GetDataType<USerializableObject>() where USerializableObject : TSerializableObject
        {
            return GetDataType(typeof(USerializableObject));
        }

        public async Task<List<USerializableObject>?> GetSerializableObjects<USerializableObject>(bool inheritance = true) where USerializableObject : TSerializableObject
        {
            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await Query.SerializableObjects<USerializableObject>(npgsqlConnection, inheritance);
        }

        public async Task<USerializableObject?> GetSerializableObject<USerializableObject>(IUniqueReference? uniqueReference) where USerializableObject : TSerializableObject
        {
            if (uniqueReference is null)
            {
                return default;
            }

            List<USerializableObject>? serializableObjects = await GetSerializableObjects<USerializableObject, IUniqueReference>([uniqueReference]);
            if (serializableObjects is null || serializableObjects.Count == 0)
            {
                return default;
            }

            return serializableObjects[0];
        }

        public async Task<List<USerializableObject>?> GetSerializableObjects<USerializableObject, TUniqueReference>(IEnumerable<TUniqueReference>? uniqueReferences) where USerializableObject : TSerializableObject where TUniqueReference : IUniqueReference
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

            return await Query.SerializableObjects<USerializableObject, TUniqueReference>(npgsqlConnection, uniqueReferences);
        }

        public async Task<TUniqueReference?> RemoveAsync<TUniqueReference>(TUniqueReference? uniqueReference) where TUniqueReference : IUniqueReference
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

        public async Task<List<TUniqueReference>?> RemoveAsync<TUniqueReference>(IEnumerable<TUniqueReference>? uniqueReferences) where TUniqueReference : IUniqueReference
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

            return await Modify.RemoveAsync(npgsqlConnection, uniqueReferences);
        }

        public async Task<bool> RemoveAsync(Type? type, bool inheritance = true)
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

            return await Modify.RemoveAsync(npgsqlConnection, type, inheritance);
        }

        public async Task<bool> RemoveAsync<USerializableObject>(bool inheritance = true) where USerializableObject : TSerializableObject
        {
            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            npgsqlConnection.Open();

            return await Modify.RemoveAsync(npgsqlConnection, typeof(USerializableObject), inheritance);
        }

        public async Task<Core.Classes.UniqueReference?> UpdateAsync(TSerializableObject? serializableObject)
        {
            if (serializableObject is null)
            {
                return null;
            }

            HashSet<Core.Classes.UniqueReference>? uniqueReferences = await UpdateAsync([serializableObject]);
            if (uniqueReferences is null || uniqueReferences.Count == 0)
            {
                return null;
            }

            return uniqueReferences.First();
        }

        public async Task<HashSet<Core.Classes.UniqueReference>?> UpdateAsync<USerializableObject>(IEnumerable<USerializableObject>? serializableObjects) where USerializableObject : TSerializableObject
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

            return await Modify.UpdateAsync(npgsqlConnection, serializableObjects, GetDataType, this, UniqueIdReferenceGenerating);
        }
    }

    public class UniqueReferencePostgreSQLConverter : UniqueReferencePostgreSQLConverter<ISerializableObject>
    {
        public UniqueReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }
    }
}