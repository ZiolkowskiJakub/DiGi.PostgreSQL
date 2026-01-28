using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using System.Collections.Generic;

namespace DiGi.PostgreSQL
{
    public static partial class Convert
    {
        public static List<TSerializableObject> ToDiGi<TSerializableObject>(this ConnectionData connectionData) where TSerializableObject : ISerializableObject
        {
            //if (serializableObject == null)
            //{
            //    return false;
            //}

            //return ToDiGi([serializableObject], connectionData);

            throw new System.NotImplementedException();
        }
    }
}