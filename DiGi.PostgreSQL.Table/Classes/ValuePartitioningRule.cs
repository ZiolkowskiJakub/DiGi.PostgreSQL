using System.Text.Json.Nodes;

namespace DiGi.PostgreSQL.Table.Classes
{
    public class ValuePartitioningRule : PartitioningRule
    {
        public ValuePartitioningRule()
            : base()
        {

        }

        public ValuePartitioningRule(ValuePartitioningRule valuePartitioningRule)
            : base(valuePartitioningRule)
        {

        }

        public ValuePartitioningRule(JsonObject jsonObject)
            : base(jsonObject)
        {

        }

    }
}
