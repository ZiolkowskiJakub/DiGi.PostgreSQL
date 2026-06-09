using System.Text.Json.Nodes;

namespace DiGi.PostgreSQL.Table.Classes
{
    /// <summary>
    /// Represents a partitioning rule based on specific values.
    /// </summary>
    public class ValuePartitioningRule : PartitioningRule
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ValuePartitioningRule"/> class.
        /// </summary>
        public ValuePartitioningRule()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ValuePartitioningRule"/> class using an existing partitioning rule.
        /// </summary>
        /// <param name="valuePartitioningRule">The source partitioning rule to copy from.</param>
        public ValuePartitioningRule(ValuePartitioningRule valuePartitioningRule)
            : base(valuePartitioningRule)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ValuePartitioningRule"/> class using a JSON object.
        /// </summary>
        /// <param name="jsonObject">The JSON object containing the rule definition.</param>
        public ValuePartitioningRule(JsonObject jsonObject)
            : base(jsonObject)
        {
        }
    }
}