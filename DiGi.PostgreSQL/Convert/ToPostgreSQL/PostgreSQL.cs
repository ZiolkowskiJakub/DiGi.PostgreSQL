using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DiGi.PostgreSQL
{
    public static partial class Convert
    {
        public static string? ToPostgreSQL(this ISerializableObject? serializableObject, ConnectionData connectionData)
        {
            if (serializableObject == null)
            {
                return null;
            }

            return ToPostgreSQL([serializableObject], connectionData)?.FirstOrDefault();
        }

        public static List<string>? ToPostgreSQL<TSerializableObject>(this IEnumerable<TSerializableObject>? serializableObjects, ConnectionData? connectionData) where TSerializableObject : ISerializableObject
        {
            throw new NotImplementedException();
        }
    }
}