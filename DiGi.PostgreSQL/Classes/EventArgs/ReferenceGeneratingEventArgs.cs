using System;

namespace DiGi.PostgreSQL.Classes
{
    public abstract class ReferenceGeneratingEventArgs : EventArgs
    {
        protected bool handled = false;

        public ReferenceGeneratingEventArgs(object? item)
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
    }
}