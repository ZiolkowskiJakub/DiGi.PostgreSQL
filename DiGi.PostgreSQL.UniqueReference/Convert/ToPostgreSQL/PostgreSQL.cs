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
        public static async Task<Core.Classes.UniqueReference?> ToPostgreSQL(this ISerializableObject? serializableObject, ConnectionData connectionData)
        {
            if (serializableObject == null)
            {
                return null;
            }

            return (await ToPostgreSQL([serializableObject], connectionData))?.FirstOrDefault();
        }

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