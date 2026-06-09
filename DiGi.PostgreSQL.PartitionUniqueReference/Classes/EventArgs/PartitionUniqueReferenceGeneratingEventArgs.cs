using DiGi.Core.Classes;

namespace DiGi.PostgreSQL.PartitionUniqueReference.Classes
{
    /// <summary>
    /// Provides data for events that occur during the generation of a partition unique reference.
    /// </summary>
    public class PartitionUniqueReferenceGeneratingEventArgs : PostgreSQL.Classes.ReferenceGeneratingEventArgs
    {
        private PartitionReference.Classes.PartitionReference? partitionReference = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionUniqueReferenceGeneratingEventArgs"/> class.
        /// </summary>
        /// <param name="item">The item for which the reference is being generated.</param>
        public PartitionUniqueReferenceGeneratingEventArgs(object? item)
            : base(item)
        {
        }

        /// <summary>
        /// Gets or sets the partition reference associated with this event.
        /// </summary>
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

        /// <summary>
        /// Gets the generated partition unique reference based on the provided item and partition reference.
        /// </summary>
        public PartitionUniqueReference? PartitionUniqueReference
        {
            get
            {
                if (partitionReference is null || Item?.GetType() is not System.Type type)
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