namespace DiGi.PostgreSQL.PartitionReference.Constants
{
    /// <summary>
    /// Discriminator tokens for the reference types defined in DiGi.PostgreSQL.PartitionReference.
    /// <para>These values are a persisted contract: they are written into stored reference strings, so they are
    /// append-only. Renaming one silently invalidates every string already stored in that format. A token must be
    /// unique across every repository, and must contain neither a comma (which would make it parse as a full type
    /// name) nor a colon.</para>
    /// <para>This class is deliberately NOT named Reference. It replaces a Constants/Reference.cs that declared its
    /// own <c>Separator = "-&gt;"</c> and, by innermost-namespace lookup, silently shadowed
    /// DiGi.Core.Constants.Reference for every type in this namespace - which is why the partition references used a
    /// different grammar from the rest of the codebase. Do not re-create a local Constants.Reference here.</para>
    /// </summary>
    public static class ReferenceKind
    {
        /// <summary>Discriminator for <see cref="Classes.PartitionReference"/>.</summary>
        public const string Partition = "Partition";
    }
}
