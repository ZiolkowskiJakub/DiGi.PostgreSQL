using DiGi.Core.Classes;
using System;

namespace DiGi.PostgreSQL.PartitionUniqueReference.Classes
{
    public class PartitionUniqueReferenceGeneratingEventArgs : PostgreSQL.Classes.ReferenceGeneratingEventArgs
    {
        private PartitionReference.Classes.PartitionReference? partitionReference = null;

        public PartitionUniqueReferenceGeneratingEventArgs(object? item)
            : base(item)
        {
        }

        public PartitionReference.Classes.PartitionReference? PartitionReference
        {
            get
            {
                return partitionReference;
            }
            set
            {
                partitionReference = value;
                handled = true;
            }
        }

        public PartitionUniqueReference? PartitionUniqueReference
        {
            get
            {
                if (partitionReference is null || Item?.GetType() is not Type type)
                {
                    return null;
                }

                if (Core.Create.TypeReference(type) is not TypeReference typeReference)
                {
                    return null;
                }

                return new PartitionUniqueReference(partitionReference.Name, new UniqueIdReference(typeReference, partitionReference.UniqueId));
            }
        }
    }
}