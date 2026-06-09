using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.UniqueReference.Classes;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Convert
    {
        /// <summary>
        /// Converts the database objects to a list of serializable objects of type <typeparamref name="TSerializableObject"/>.
        /// </summary>
        /// <typeparam name="TSerializableObject">The type of serializable object to convert to.</typeparam>
        /// <param name="connectionData">The connection data used to connect to the database.</param>
        /// <param name="inheritance">A value indicating whether to include inherited types.</param>
        /// <returns>A task representing the asynchronous operation, returning the list of serializable objects or null.</returns>
        public static async Task<List<TSerializableObject>?> ToDiGi<TSerializableObject>(this ConnectionData connectionData, bool inheritance = true) where TSerializableObject : ISerializableObject
        {
            if (connectionData is null)
            {
                return null;
            }

            return await new UniqueReferencePostgreSQLConverter(connectionData).GetSerializableObjectsAsync<TSerializableObject>(inheritance);
        }

        /// <summary>
        /// Converts the database objects with the specified unique references to a list of serializable objects of type <typeparamref name="TSerializableObject"/>.
        /// </summary>
        /// <typeparam name="TSerializableObject">The type of serializable object to convert to.</typeparam>
        /// <typeparam name="TUniqueReference">The type of unique reference.</typeparam>
        /// <param name="connectionData">The connection data used to connect to the database.</param>
        /// <param name="uniqueReferences">The collection of unique references to filter by.</param>
        /// <returns>A task representing the asynchronous operation, returning the list of serializable objects or null.</returns>
        public static async Task<List<TSerializableObject>?> ToDiGi<TSerializableObject, TUniqueReference>(this ConnectionData connectionData, IEnumerable<TUniqueReference> uniqueReferences) where TSerializableObject : ISerializableObject where TUniqueReference : IUniqueReference
        {
            if (connectionData is null)
            {
                return null;
            }

            return await new UniqueReferencePostgreSQLConverter(connectionData).GetSerializableObjectsAsync<TSerializableObject>();
        }
    }
}