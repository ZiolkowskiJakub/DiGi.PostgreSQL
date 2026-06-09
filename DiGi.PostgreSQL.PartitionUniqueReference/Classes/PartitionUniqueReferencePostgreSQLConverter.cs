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
    /// <summary>
    /// Provides functionality to convert and manage partition unique references within a PostgreSQL database for objects implementing <see cref="ISerializableObject"/>.
    /// </summary>
    /// <typeparam name="TSerializableObject">The type of the serializable object, which must implement <see cref="ISerializableObject"/>.</typeparam>
    public class PartitionUniqueReferencePostgreSQLConverter<TSerializableObject> : PostgreSQLConverter<TSerializableObject> where TSerializableObject : ISerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionUniqueReferencePostgreSQLConverter{TSerializableObject}"/> class.
        /// </summary>
        /// <param name="connectionData">The connection data used to establish a database connection.</param>
        public PartitionUniqueReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }

        /// <summary>
        /// Occurs when a partition unique reference is being generated.
        /// </summary>
        public event PartitionUniqueReferenceGeneratingEventHandler? PartitionUniqueReferenceReferenceGenerating;

        /// <summary>
        /// Cleans the specified partitions and types from the database.
        /// </summary>
        /// <param name="partitions">A value indicating whether to clean partitions.</param>
        /// <param name="types">A value indicating whether to clean types.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains true if any partitions or types were cleaned; otherwise, false.</returns>
        public async Task<bool> Clean(bool partitions = true, bool types = true)
        {
            await using NpgsqlConnection? npgsqlConnection = Create.NpgsqlConnection(ConnectionData);
            if (npgsqlConnection is null)
            {
                return false;
            }

            npgsqlConnection.Open();

            List<Partition>? partitions_Result = null;

            if (partitions)
            {
                partitions_Result = await PostgreSQL.Modify.CleanPartitionsAsync(npgsqlConnection);
            }

            List<Type>? types_Result = null;
            if (types)
            {
                types_Result = await Modify.CleanTypesAsync(npgsqlConnection);
            }

            return (partitions_Result != null && partitions_Result.Count != 0) || (types_Result != null && types_Result.Count != 0);
        }

        /// <summary>
        /// Checks asynchronously whether the specified type exists in the database.
        /// </summary>
        /// <param name="type">The type to check for existence.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains true if the type exists; otherwise, false.</returns>
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
        /// Checks asynchronously whether the specified system type exists in the database.
        /// </summary>
        /// <param name="type">The system type to check for existence.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains true if the type exists; otherwise, false.</returns>
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

        /// <summary>
        /// Checks asynchronously whether the specified collection of partition unique references exists in the database.
        /// </summary>
        /// <param name="partitionUniqueReferences">The collection of unique references to check.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a hash set of existing unique references, or null if the input was null or connection failed.</returns>
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

        /// <summary>
        /// Gets the data type associated with the specified name.
        /// </summary>
        /// <param name="name">The name of the entity to determine the data type for.</param>
        /// <returns>The <see cref="DataType"/> associated with the name, or <see cref="DataType.Undefined"/> if the name is null or whitespace.</returns>
        public virtual DataType GetDataType(string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return DataType.Undefined;
            }

            return DataType.Json;
        }

        /// <summary>
        /// Retrieves a single serializable object asynchronously based on the provided unique reference.
        /// </summary>
        /// <typeparam name="USerializableObject">The specific type of the serializable object.</typeparam>
        /// <param name="partitionUniqueReference">The unique reference used to locate the object.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the found object, or default if not found or input is null.</returns>
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

        /// <summary>
        /// Retrieves a list of serializable objects asynchronously based on the provided collection of unique references.
        /// </summary>
        /// <typeparam name="USerializableObject">The specific type of the serializable objects.</typeparam>
        /// <param name="partitionUniqueReferences">The collection of unique references used to locate the objects.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of found objects, or null if input is null or connection failed.</returns>
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

        /// <summary>
        /// Removes the specified collection of unique references from the database asynchronously.
        /// </summary>
        /// <param name="partitionUniqueReferences">The collection of unique references to remove.</param>
        /// <param name="clean">A value indicating whether to perform a cleaning operation after removal.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a hash set of removed unique references, or null if input is null or connection failed.</returns>
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

        /// <summary>
        /// Removes a single unique reference from the database asynchronously.
        /// </summary>
        /// <param name="partitionUniqueReference">The unique reference to remove.</param>
        /// <param name="clean">A value indicating whether to perform a cleaning operation after removal.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the removed unique reference, or null if not found or input is null.</returns>
        public async Task<PartitionUniqueReference?> RemoveAsync(PartitionUniqueReference? partitionUniqueReference, bool clean = true)
        {
            if (partitionUniqueReference is null)
            {
                return null;
            }

            HashSet<PartitionUniqueReference>? partitionUniqueReferences = await RemoveAsync([partitionUniqueReference], clean);
            if (partitionUniqueReferences is null || partitionUniqueReferences.Count == 0)
            {
                return null;
            }

            return partitionUniqueReferences.First();
        }

        /// <summary>
        /// Updates a single serializable object in the database asynchronously.
        /// </summary>
        /// <param name="serializableObject">The object to update.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the unique reference of the updated object, or null if input is null or update failed.</returns>
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

        /// <summary>
        /// Updates a collection of serializable objects in the database asynchronously.
        /// </summary>
        /// <typeparam name="USerializableObject">The specific type of the serializable objects.</typeparam>
        /// <param name="serializableObjects">The collection of objects to update.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a hash set of unique references for the updated objects, or null if input is null or connection failed.</returns>
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

    /// <summary>
    /// A non-generic implementation of the partition unique reference converter using <see cref="ISerializableObject"/>.
    /// </summary>
    public class PartitionUniqueReferencePostgreSQLConverter : PartitionUniqueReferencePostgreSQLConverter<ISerializableObject>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionUniqueReferencePostgreSQLConverter"/> class.
        /// </summary>
        /// <param name="connectionData">The connection data used to establish a database connection.</param>
        public PartitionUniqueReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }
    }
}