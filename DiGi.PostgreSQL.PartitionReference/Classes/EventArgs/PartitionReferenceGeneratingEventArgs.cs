namespace DiGi.PostgreSQL.PartitionReference.Classes
{
    /// <summary>
    /// Provides data for events that occur during the generation of a partition reference.
    /// </summary>
    public class PartitionReferenceGeneratingEventArgs : PostgreSQL.Classes.ReferenceGeneratingEventArgs
    {
        private PartitionReference? partitionReference = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionReferenceGeneratingEventArgs"/> class.
        /// </summary>
        /// <param name="item">The item associated with the event.</param>
        public PartitionReferenceGeneratingEventArgs(object? item)
            : base(item)
        {
        }

        /// <summary>
        /// Gets or sets the partition reference associated with this event.
        /// </summary>
        public PartitionReference? PartitionReference
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
    }
}