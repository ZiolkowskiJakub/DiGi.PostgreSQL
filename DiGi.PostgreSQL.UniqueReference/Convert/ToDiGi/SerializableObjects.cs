using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using DiGi.PostgreSQL.UniqueReference.Classes;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DiGi.PostgreSQL.UniqueReference
{
    public static partial class Convert
    {
        public static async Task<List<TSerializableObject>?> ToDiGi<TSerializableObject>(this ConnectionData connectionData, bool inheritance = true) where TSerializableObject : ISerializableObject
        {
            if (connectionData is null)
            {
                return null;
            }

            return await new UniqueReferencePostgreSQLConverter(connectionData).GetSerializableObjects<TSerializableObject>(inheritance);
        }

        public static async Task<List<TSerializableObject>?> ToDiGi<TSerializableObject, TUniqueReference>(this ConnectionData connectionData, IEnumerable<TUniqueReference> uniqueReferences) where TSerializableObject : ISerializableObject where TUniqueReference : IUniqueReference
        {
            if (connectionData is null)
            {
                return null;
            }

            return await new UniqueReferencePostgreSQLConverter(connectionData).GetSerializableObjects<TSerializableObject>();
        }
    }
}