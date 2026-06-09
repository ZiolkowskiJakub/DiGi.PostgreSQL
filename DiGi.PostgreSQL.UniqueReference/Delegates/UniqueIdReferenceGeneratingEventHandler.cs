using DiGi.PostgreSQL.UniqueReference.Classes;

namespace DiGi.PostgreSQL.UniqueReference.Delegates
{
    /// <summary>
    /// Represents the method that will handle the event when a unique ID reference is being generated.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">A <see cref="UniqueIdReferenceGeneratingEventArgs"/> object that contains the event data.</param>
    public delegate void UniqueIdReferenceGeneratingEventHandler(object sender, UniqueIdReferenceGeneratingEventArgs e);
}