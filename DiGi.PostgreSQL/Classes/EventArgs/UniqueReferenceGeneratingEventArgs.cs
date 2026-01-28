using DiGi.Core.Classes;
using System;

namespace DiGi.PostgreSQL.Classes
{
    public class UniqueReferenceGeneratingEventArgs : EventArgs
    {
        protected bool handled = false;

        private UniqueReference? uniqueReference = null;

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

        public UniqueReference? UniqueReference
        {
            get
            {
                return uniqueReference;
            }

            set
            {
                uniqueReference = value;
                handled = true;
            }
        }
    }
}