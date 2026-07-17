using DiGi.Core.Classes;
using DiGi.Core.Interfaces;
using System.Collections.Generic;

namespace DiGi.PostgreSQL.PartitionUniqueReference
{
    public static partial class Create
    {
        /// <summary>Rebuilds a <see cref="Classes.PartitionUniqueReference"/> from the segments of its string form.</summary>
        /// <param name="segments">The segments: the partition name, then the nested unique reference.</param>
        /// <returns>The reference, or <c>null</c> if the segments do not describe one.</returns>
        [ReferenceFactory(typeof(Classes.PartitionUniqueReference), Kind = Constants.ReferenceKind.PartitionUnique)]
        public static IReference? PartitionUniqueReference(IReadOnlyList<string?>? segments)
        {
            if (segments == null || segments.Count != 2)
            {
                return null;
            }

            // The nested reference is optional - the class permits a null unique reference, which renders as the null
            // token. Only a present-but-unresolvable slot is a failure; the null token rebuilds a null reference.
            IUniqueReference? uniqueReference = null;
            if (segments[1] != Core.Constants.Reference.Null)
            {
                // The nested slot is polymorphic across every unique reference, including ones defined in other
                // repositories, so resolution goes back through Core rather than assuming a concrete type.
                if (Core.Query.Reference(segments[1]) is not IUniqueReference uniqueReference_Temp)
                {
                    return null;
                }

                uniqueReference = uniqueReference_Temp;
            }

            return new Classes.PartitionUniqueReference(Core.Query.Unescaped(segments[0]), uniqueReference);
        }
    }
}
