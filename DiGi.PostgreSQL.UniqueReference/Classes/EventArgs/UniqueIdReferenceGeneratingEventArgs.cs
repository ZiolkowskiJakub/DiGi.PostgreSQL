using DiGi.Core.Classes;
using System;

namespace DiGi.PostgreSQL.UniqueReference.Classes
{
    public class UniqueIdReferenceGeneratingEventArgs : PostgreSQL.Classes.ReferenceGeneratingEventArgs
    {
        private string? uniqueId = null;

        public UniqueIdReferenceGeneratingEventArgs(object? item)
            : base(item)
        {
        }

        public string? UniqueId
        {
            get
            {
                return uniqueId;
            }
            set
            {
                uniqueId = value;
                handled = true;
            }
        }

        public UniqueIdReference? UniqueIdReference
        {
            get
            {
                if (uniqueId is null || Item?.GetType() is not Type type)
                {
                    return null;
                }

                if (Core.Create.TypeReference(type) is not TypeReference typeReference)
                {
                    return null;
                }

                return new UniqueIdReference(typeReference, uniqueId);
            }
        }
    }
}