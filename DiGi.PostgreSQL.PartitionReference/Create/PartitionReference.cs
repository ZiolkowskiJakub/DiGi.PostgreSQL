using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using System.Collections.Generic;

namespace DiGi.PostgreSQL.PartitionReference
{
    public static partial class Create
    {
        /// <summary>Rebuilds a <see cref="Classes.PartitionReference"/> from the segments of its string form.</summary>
        /// <param name="segments">The segments: the partition name, then the unique identifier.</param>
        /// <returns>The reference, or <c>null</c> if the segments do not describe one.</returns>
        [ReferenceFactory(typeof(Classes.PartitionReference), Kind = Constants.ReferenceKind.Partition)]
        public static IReference? PartitionReference(IReadOnlyList<string?>? segments)
        {
            if (segments == null || segments.Count != 2)
            {
                return null;
            }

            return new Classes.PartitionReference(Core.Query.Unescaped(segments[0]), Core.Query.Unescaped(segments[1]));
        }
    }
}
