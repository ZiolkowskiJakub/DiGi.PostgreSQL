using DiGi.Core.Classes;
using System;

namespace DiGi.PostgreSQL.Classes
{
    public class UniqueReferenceGeneratingEventArgs : EventArgs
    {
        protected bool handled = false;

        private string? uniqueId = null;

        public UniqueReferenceGeneratingEventArgs(object? item)
        {
            Item = item;
        }

        public bool Handled
        {
            get
            {
                return handled;
            }
        }

        public object? Item { get; }

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