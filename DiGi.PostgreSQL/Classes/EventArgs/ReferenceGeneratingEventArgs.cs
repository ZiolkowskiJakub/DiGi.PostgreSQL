using System;

namespace DiGi.PostgreSQL.Classes
{
    /// <summary>
    /// Provides data for events that occur during reference generation.
    /// </summary>
    public abstract class ReferenceGeneratingEventArgs : EventArgs
    {
        /// <summary>
        /// Indicates whether the event has been handled.
        /// </summary>
        protected bool handled = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="ReferenceGeneratingEventArgs"/> class.
        /// </summary>
        /// <param name="item">The item associated with the reference generation process.</param>
        public ReferenceGeneratingEventArgs(object? item)
        {
            Item = item;
        }

        /// <summary>
        /// Gets a value indicating whether the event has been handled.
        /// </summary>
        public bool Handled
        {
            get
            {
                return handled;
            }
        }

        /// <summary>
        /// Gets the item associated with the reference generation process.
        /// </summary>
        public object? Item { get; }
    }
}