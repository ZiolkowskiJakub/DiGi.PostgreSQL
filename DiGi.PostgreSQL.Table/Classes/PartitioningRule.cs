using DiGi.Core.Classes;
using System.Text.Json.Nodes;

namespace DiGi.PostgreSQL.Table.Classes
{
    public abstract class PartitioningRule : SerializableObject
    {
        public PartitioningRule()
            : base()
        {
        }

        public PartitioningRule(PartitioningRule partitioningRule)
            : base(partitioningRule)
        {
        }

        public PartitioningRule(JsonObject jsonObject)
            : base(jsonObject)
        {
        }
    }
}