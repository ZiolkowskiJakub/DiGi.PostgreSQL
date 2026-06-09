using DiGi.Core.Classes;
using System.Text.Json.Nodes;

namespace DiGi.PostgreSQL.Table.Classes
{
    /// <summary>
    /// Represents an abstract base class for defining partitioning rules in a PostgreSQL table.
    /// </summary>
    public abstract class PartitioningRule : SerializableObject
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitioningRule"/> class.
        /// </summary>
        public PartitioningRule()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitioningRule"/> class by copying an existing partitioning rule.
        /// </summary>
        /// <param name="partitioningRule">The source partitioning rule to copy from.</param>
        public PartitioningRule(PartitioningRule partitioningRule)
            : base(partitioningRule)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitioningRule"/> class using a JSON object.
        /// </summary>
        /// <param name="jsonObject">The JSON object containing the partitioning rule data.</param>
        public PartitioningRule(JsonObject jsonObject)
            : base(jsonObject)
        {
        }
    }
}