namespace DiGi.PostgreSQL.PartitionUniqueReference.Delegates
{
    /// <summary>
    /// Represents the method that will handle the partition unique reference generating event.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">A <see cref="Classes.PartitionUniqueReferenceGeneratingEventArgs"/> that contains the event data.</param>
    public delegate void PartitionUniqueReferenceGeneratingEventHandler(object sender, Classes.PartitionUniqueReferenceGeneratingEventArgs e);
}