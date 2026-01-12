using DiGi.Core.Interfaces;
using DiGi.PostgreSQL.Classes;
using System.Collections.Generic;

namespace DiGi.Core
{
    public static partial class Convert
    {
        public static bool ToPostgreSQL(this ISerializableObject? serializableObject, ConnectionData connectionData)
        {
            if (serializableObject == null)
            {
                return false;
            }

            return ToPostgreSQL([serializableObject], connectionData);
        }

        public static bool ToPostgreSQL<TSerializableObject>(this IEnumerable<TSerializableObject>? serializableObjects, ConnectionData? connectionData) where TSerializableObject : ISerializableObject
        {
            if(serializableObjects is null || connectionData is null)
            {
                return false;
            }

            Dictionary<string, List<TSerializableObject>> dictionary = [];

            foreach (TSerializableObject serializableObject in serializableObjects)
            {
                if(serializableObject?.GetType()?.Name is not string name)
                {
                    continue; 
                }

                if(!dictionary.TryGetValue(name, out List<TSerializableObject>? serializableObjects_Name) || serializableObjects_Name is null)
                {
                    serializableObjects_Name = [];
                    dictionary[name] = serializableObjects_Name;
                }

                serializableObjects_Name.Add(serializableObject);
            }

            if(dictionary.Count == 0)
            {
                return false;
            }

            return false;       
        }
    }
}
