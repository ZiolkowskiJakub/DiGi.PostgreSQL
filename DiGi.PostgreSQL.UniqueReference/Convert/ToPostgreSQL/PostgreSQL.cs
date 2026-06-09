using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.UniqueReference.Classes;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Convert
    {
        /// <summary>
        /// Converts a serializable object to its PostgreSQL unique reference representation.
        /// </summary>
        /// <param name="serializableObject">The serializable object to convert.</param>
        /// <param name="connectionData">The connection data for the PostgreSQL database.</param>
        /// <returns>A task that represents the asynchronous operation, containing the converted unique reference or null if the input object is null.</returns>
        public static async Task<Core.Classes.UniqueReference?> ToPostgreSQL(this ISerializableObject? serializableObject, ConnectionData connectionData)
        {
            if (serializableObject == null)
            {
                return null;
            }

            return (await ToPostgreSQL([serializableObject], connectionData))?.FirstOrDefault();
        }

        /// <summary>
        /// Converts a collection of serializable objects to their PostgreSQL unique reference representations.
        /// </summary>
        /// <typeparam name="TSerializableObject">The type of the serializable objects, which must implement ISerializableObject.</typeparam>
        /// <param name="serializableObjects">The collection of serializable objects to convert.</param>
        /// <param name="connectionData">The connection data for the PostgreSQL database.</param>
        /// <returns>A task that represents the asynchronous operation, containing a hash set of converted unique references or null if inputs are null.</returns>
        public static async Task<HashSet<Core.Classes.UniqueReference>?> ToPostgreSQL<TSerializableObject>(this IEnumerable<TSerializableObject>? serializableObjects, ConnectionData? connectionData) where TSerializableObject : ISerializableObject
        {
            if (serializableObjects is null || connectionData is null)
            {
                return null;
            }

            return await new UniqueReferencePostgreSQLConverter(connectionData).UpdateAsync(serializableObjects);
        }
    }
}