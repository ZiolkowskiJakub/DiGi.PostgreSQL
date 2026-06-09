using DiGi.PostgreSQL.PartitionReference.Classes;

namespace DiGi.PostgreSQL.PartitionReference.Delegates
{
    /// <summary>
    /// Represents the method that will handle the event when a partition reference is being generated.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">A <see cref="PartitionReferenceGeneratingEventArgs"/> that contains the event data.</param>
    public delegate void PartitionReferenceGeneratingEventHandler(object sender, PartitionReferenceGeneratingEventArgs e);
}