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
    /// <summary>
    /// Provides a converter for managing partition references within a PostgreSQL database for serializable objects.
    /// </summary>
    /// <typeparam name="TSerializableObject">The type of the serializable object that implements ISerializableObject.</typeparam>
    public class PartitionReferencePostgreSQLConverter<TSerializableObject> : PostgreSQLConverter<TSerializableObject> where TSerializableObject : ISerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionReferencePostgreSQLConverter{TSerializableObject}"/> class.
        /// </summary>
        /// <param name="connectionData">The connection data used to establish a database connection.</param>
        public PartitionReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }

        /// <summary>
        /// Event that is triggered when a partition reference is being generated.
        /// </summary>
        public event PartitionReferenceGeneratingEventHandler? PartitionReferenceGenerating;

        /// <summary>
        /// Asynchronously checks if the specified partition references exist in the database.
        /// </summary>
        /// <typeparam name="TUniqueReference">The type of the unique reference used for the check.</typeparam>
        /// <param name="partitionReferences">A collection of partition references to verify.</param>
        /// <returns>A hash set containing the existing partition references, or null if the input is null or connection fails.</returns>
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

        /// <summary>
        /// Asynchronously counts the number of elements associated with a specific partition name.
        /// </summary>
        /// <param name="name">The name of the partition to count.</param>
        /// <returns>The total count of elements, or -1 if the name is null or an error occurs.</returns>
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

        /// <summary>
        /// Retrieves the data type associated with a given partition name.
        /// </summary>
        /// <param name="name">The name of the partition.</param>
        /// <returns>The <see cref="DataType"/> of the partition, or DataType.Undefined if the name is null or whitespace.</returns>
        public virtual DataType GetDataType(string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return DataType.Undefined;
            }

            return DataType.Json;
        }

        /// <summary>
        /// Asynchronously retrieves a single serializable object associated with the specified partition reference.
        /// </summary>
        /// <typeparam name="USerializableObject">The specific type of the serializable object.</typeparam>
        /// <param name="partitionReference">The partition reference to retrieve the object for.</param>
        /// <returns>The retrieved serializable object, or default if not found or input is null.</returns>
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

        /// <summary>
        /// Asynchronously retrieves a list of serializable objects associated with the specified partition references.
        /// </summary>
        /// <typeparam name="USerializableObject">The specific type of the serializable object.</typeparam>
        /// <param name="partitionReferences">A collection of partition references to retrieve objects for.</param>
        /// <returns>A list of retrieved serializable objects, or null if input is null or connection fails.</returns>
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

        /// <summary>
        /// Asynchronously retrieves a list of serializable objects associated with the specified partition name.
        /// </summary>
        /// <typeparam name="USerializableObject">The specific type of the serializable object.</typeparam>
        /// <param name="name">The name of the partition.</param>
        /// <returns>A list of retrieved serializable objects, or default if input is null or connection fails.</returns>
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

        /// <summary>
        /// Asynchronously removes a single partition reference from the database.
        /// </summary>
        /// <param name="partitionReference">The partition reference to remove.</param>
        /// <returns>The removed partition reference, or default if not found or input is null.</returns>
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

        /// <summary>
        /// Asynchronously removes a collection of partition references from the database.
        /// </summary>
        /// <param name="partitionReferences">A collection of partition references to remove.</param>
        /// <returns>A hash set containing the removed partition references, or null if input is null or connection fails.</returns>
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

        /// <summary>
        /// Asynchronously removes a partition reference identified by its name.
        /// </summary>
        /// <param name="name">The name of the partition to remove.</param>
        /// <returns>True if the partition was successfully removed, otherwise false.</returns>
        public async Task<bool> RemoveAsync(string? name)
        {
            if (name is null)
            {
                return false;
            }

            HashSet<string>? names = await RemoveAsync([name]);

            return names != null && names.Contains(name);
        }

        /// <summary>
        /// Asynchronously removes multiple partition references identified by their names.
        /// </summary>
        /// <param name="names">A collection of partition names to remove.</param>
        /// <returns>A hash set containing the names of successfully removed partitions, or null if input is null.</returns>
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

        /// <summary>
        /// Asynchronously updates a single serializable object in the database.
        /// </summary>
        /// <param name="serializableObject">The serializable object to update.</param>
        /// <returns>The updated partition reference, or null if input is null or update fails.</returns>
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

        /// <summary>
        /// Asynchronously updates a collection of serializable objects in the database.
        /// </summary>
        /// <typeparam name="USerializableObject">The specific type of the serializable object.</typeparam>
        /// <param name="serializableObjects">A collection of serializable objects to update.</param>
        /// <returns>A hash set containing the updated partition references, or null if input is null or connection fails.</returns>
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

    /// <summary>
    /// A non-generic implementation of the partition reference PostgreSQL converter using ISerializableObject.
    /// </summary>
    public class PartitionReferencePostgreSQLConverter : PartitionReferencePostgreSQLConverter<ISerializableObject>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionReferencePostgreSQLConverter"/> class.
        /// </summary>
        /// <param name="connectionData">The connection data used to establish a database connection.</param>
        public PartitionReferencePostgreSQLConverter(ConnectionData? connectionData)
            : base(connectionData)
        {
        }
    }
}