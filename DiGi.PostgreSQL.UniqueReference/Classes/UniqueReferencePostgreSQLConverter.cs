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
    /// <summary>
    /// Provides a PostgreSQL converter implementation specifically designed to handle unique references for objects that implement the <see cref="ISerializableObject"/> interface.
    /// </summary>
    /// <typeparam name="TSerializableObject">The type of the serializable object being converted, which must implement the <see cref="ISerializableObject"/> interface.</typeparam>
    public class UniqueReferencePostgreSQLConverter<TSerializableObject> : PostgreSQLConverter<TSerializableObject> where TSerializableObject : ISerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="UniqueReferencePostgreSQLConverter"/> class.
        /// </summary>
        /// <param name="connectionData">The connection data used to configure the PostgreSQL database connection; can be null.</param>
        public UniqueReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }

        /// <summary>
        /// Occurs when a unique identifier reference is being generated.
        /// </summary>
        public event UniqueIdReferenceGeneratingEventHandler? UniqueIdReferenceGenerating;

        /// <summary>
        /// Asynchronously determines whether the container contains the specified type.
        /// </summary>
        /// <param name="type">The <see cref="Type"/> to locate in the container. This value can be null.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the specified type is found; otherwise, <see langword="false"/>.</returns>
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

        /// <summary>
        /// Asynchronously checks for the existence of a collection of unique references and returns those that are present.
        /// </summary>
        /// <typeparam name="TUniqueReference">The type of the unique reference, which must implement the <see cref="IUniqueReference"/> interface.</typeparam>
        /// <param name="uniqueReferences">An optional enumerable collection of unique references to verify.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a <see cref="HashSet{TUniqueReference}"/> of existing references, or <see langword="null"/> if no references were provided or found.</returns>
        public async Task<HashSet<TUniqueReference>?> ContainsAsync<TUniqueReference>(IEnumerable<TUniqueReference>? uniqueReferences) where TUniqueReference : IUniqueReference
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

            return await Query.ContainsAsync(npgsqlConnection, uniqueReferences);
        }

        /// <summary>
        /// Asynchronously counts the number of elements associated with the specified type.
        /// </summary>
        /// <param name="type">The <see cref="Type"/> to count. If null, the behavior is determined by the underlying implementation.</param>
        /// <param name="inheritance">A value indicating whether to include types derived from the specified type in the count. Defaults to <c>true</c>.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the total count as a <see cref="long"/>.</returns>
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

        /// <summary>
        /// Asynchronously counts the total number of records for the specified serializable object type.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of the serializable object to count, which must derive from <typeparamref name="TSerializableObject"/>.</typeparam>
        /// <param name="inheritance">A value indicating whether to include types derived from <typeparamref name="USerializableObject"/> in the count. Defaults to <see langword="true"/>.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the total number of records as a <see cref="long"/>.</returns>
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

        /// <summary>
        /// Retrieves the corresponding <see cref="DataType"/> for the specified .NET type.
        /// </summary>
        /// <param name="type">The .NET type to map to a <see cref="DataType"/>. This value can be null.</param>
        /// <returns>The <see cref="DataType"/> that represents the provided type.</returns>
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

        /// <summary>
        /// Retrieves the <see cref="DataType"/> associated with the specified serializable object type.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of the serializable object, which must derive from <typeparamref name="TSerializableObject"/>.</typeparam>
        /// <returns>The <see cref="DataType"/> corresponding to the provided generic type.</returns>
        public DataType GetDataType<USerializableObject>() where USerializableObject : TSerializableObject
        {
            return GetDataType(typeof(USerializableObject));
        }

        /// <summary>
        /// Asynchronously retrieves a serializable object associated with the specified unique reference.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of the serializable object to retrieve, which must derive from <typeparamref name="TSerializableObject"/>.</typeparam>
        /// <param name="uniqueReference">The unique reference used to identify the object. May be null.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the retrieved serializable object if found; otherwise, null.</returns>
        public async Task<USerializableObject?> GetSerializableObjectAsync<USerializableObject>(IUniqueReference? uniqueReference) where USerializableObject : TSerializableObject
        {
            if (uniqueReference is null)
            {
                return default;
            }

            List<USerializableObject>? serializableObjects = await GetSerializableObjectsAsync<USerializableObject, IUniqueReference>([uniqueReference]);
            if (serializableObjects is null || serializableObjects.Count == 0)
            {
                return default;
            }

            return serializableObjects[0];
        }

        /// <summary>
        /// Asynchronously retrieves a list of serializable objects from the data store.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of serializable object to retrieve, which must derive from <typeparamref name="TSerializableObject"/>.</typeparam>
        /// <param name="inheritance">A value indicating whether to include types derived from <typeparamref name="USerializableObject"/> in the results. Defaults to <c>true</c>.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of objects of type <typeparamref name="USerializableObject"/>, or <c>null</c> if no objects are found.</returns>
        public async Task<List<USerializableObject>?> GetSerializableObjectsAsync<USerializableObject>(bool inheritance = true) where USerializableObject : TSerializableObject
        {
            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return null;
            }

            npgsqlConnection.Open();

            return await Query.SerializableObjectsAsync<USerializableObject>(npgsqlConnection, inheritance);
        }

        /// <summary>
        /// Asynchronously retrieves a list of serializable objects associated with the provided unique references.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of serializable object to retrieve, which must derive from <typeparamref name="TSerializableObject"/>.</typeparam>
        /// <typeparam name="TUniqueReference">The type of unique reference used for identification, which must implement <see cref="IUniqueReference"/>.</typeparam>
        /// <param name="uniqueReferences">An optional collection of unique references to be used as keys for retrieving the objects.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of retrieved serializable objects, or null if the input references were null or no objects could be found.</returns>
        public async Task<List<USerializableObject>?> GetSerializableObjectsAsync<USerializableObject, TUniqueReference>(IEnumerable<TUniqueReference>? uniqueReferences) where USerializableObject : TSerializableObject where TUniqueReference : IUniqueReference
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

            return await Query.SerializableObjectsAsync<USerializableObject, TUniqueReference>(npgsqlConnection, uniqueReferences);
        }

        /// <summary>
        /// Asynchronously removes an item identified by the specified unique reference.
        /// </summary>
        /// <typeparam name="TUniqueReference">The type of the unique reference, which must implement <see cref="IUniqueReference"/>.</typeparam>
        /// <param name="uniqueReference">The unique reference of the item to be removed. This value can be null.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the removed unique reference if the operation was successful; otherwise, null.</returns>
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

        /// <summary>
        /// Asynchronously removes the entities associated with the specified collection of unique references.
        /// </summary>
        /// <typeparam name="TUniqueReference">The type of the unique reference, which must implement <see cref="IUniqueReference"/>.</typeparam>
        /// <param name="uniqueReferences">An optional collection of unique references to be removed.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of the unique references that were successfully removed, or <see langword="null"/> if the input was null or no items were removed.</returns>
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

        /// <summary>
        /// Asynchronously removes the specified type from the collection.
        /// </summary>
        /// <param name="type">The <see cref="Type"/> to remove. Can be null.</param>
        /// <param name="inheritance">A value indicating whether types that inherit from the specified type should also be removed. Defaults to <c>true</c>.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <c>true</c> if the type was successfully removed; otherwise, <c>false</c>.</returns>
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

        /// <summary>
        /// Asynchronously removes an object of the specified serializable type from the data store.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of the serializable object to remove, which must derive from <typeparamref name="TSerializableObject"/>.</typeparam>
        /// <param name="inheritance">A value indicating whether the removal process should include inherited types. Defaults to <see langword="true"/>.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the object was successfully removed; otherwise, <see langword="false"/>.</returns>
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

        /// <summary>
        /// Asynchronously updates the specified serializable object in the data store.
        /// </summary>
        /// <param name="serializableObject">The serializable object instance to update. Can be null.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the <see cref="Core.Classes.UniqueReference"/> of the updated object, or null if the update could not be completed or the input was null.</returns>
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

        /// <summary>
        /// Asynchronously updates a collection of serializable objects and returns the set of unique references associated with the updated entities.
        /// </summary>
        /// <typeparam name="USerializableObject">The type of the serializable object to update, which must implement or derive from <typeparamref name="TSerializableObject"/>.</typeparam>
        /// <param name="serializableObjects">An optional collection of objects to be processed for updates.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a HashSet of UniqueReference of unique references if the update was performed; otherwise, <c>null</c>.</returns>
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

    /// <summary>
    /// Provides a specialized PostgreSQL converter for unique references specifically targeting objects that implement the <see cref="ISerializableObject"/> interface.
    /// </summary>
    public class UniqueReferencePostgreSQLConverter : UniqueReferencePostgreSQLConverter<ISerializableObject>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="UniqueReferencePostgreSQLConverter"/> class using the specified connection data.
        /// </summary>
        /// <param name="connectionData">The connection data used to configure the PostgreSQL converter; may be null.</param>
        public UniqueReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }
    }
}