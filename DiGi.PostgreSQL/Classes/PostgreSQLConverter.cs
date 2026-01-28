using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Delegates;
using DiGi.PostgreSQL.Interfaces;
using Npgsql;
using System;
using System.Collections.Generic;
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

        public async Task<USerializableObject?> GetSerializableObjects<USerializableObject>(IUniqueReference uniqueReference) where USerializableObject : TSerializableObject
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

        public async Task<List<USerializableObject>?> GetSerializableObjects<USerializableObject, TUniqueReference>(IEnumerable<TUniqueReference> uniqueReferences) where USerializableObject : TSerializableObject where TUniqueReference : IUniqueReference
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

            return await Modify.RemoveAsync(npgsqlConnection, uniqueReferences);
        }

        public async Task<bool> RemoveAsync(Type type, bool inheritance = true)
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

            return await Modify.UpdateAsync(npgsqlConnection, serializableObjects, this, UniqueReferenceGenerating);
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