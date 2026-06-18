using DiGi.Core.Classes;
using System;

namespace DiGi.PostgreSQL.UniqueReference.Classes
{
    /// <summary>
    /// Event arguments used during the generation of a unique ID reference.
    /// </summary>
    public class UniqueIdReferenceGeneratingEventArgs : PostgreSQL.Classes.ReferenceGeneratingEventArgs
    {
        private string? uniqueId = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="UniqueIdReferenceGeneratingEventArgs"/> class.
        /// </summary>
        /// <param name="item">The item for which the reference is being generated.</param>
        public UniqueIdReferenceGeneratingEventArgs(object? item)
            : base(item)
        {
        }

        /// <summary>
        /// Gets or sets the unique identifier string associated with this reference.
        /// </summary>
        public string? UniqueId
        {
            get
            {
                return uniqueId;
            }
            set
            {
                uniqueId = value;
                handled = true;
            }
        }

        /// <summary> Gets the constructed <see cref="UniqueIdReference" /> based on the item type and the provided unique identifier. </summary>
        public UniqueIdReference? UniqueIdReference
        {
            get
            {
                if (uniqueId is null || Item?.GetType() is not Type type)
                {
                    return null;
                }

                if (Core.Create.TypeReference(type) is not TypeReference typeReference)
                {
                    return null;
                }

                return new UniqueIdReference(typeReference, uniqueId);
            }
        }
    }
}
