namespace DiGi.PostgreSQL.PartitionReference.Classes
{
    public class PartitionReferenceGeneratingEventArgs : PostgreSQL.Classes.ReferenceGeneratingEventArgs
    {
        private PartitionReference? partitionReference = null;

        public PartitionReferenceGeneratingEventArgs(object? item)
            : base(item)
        {
        }

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