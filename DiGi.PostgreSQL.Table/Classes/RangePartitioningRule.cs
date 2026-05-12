using DiGi.Core.Classes;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DiGi.PostgreSQL.Table.Classes
{
    public abstract class RangePartitioningRule : PartitioningRule
    {
        public RangePartitioningRule()
            : base()
        {
        }

        public RangePartitioningRule(RangePartitioningRule rangePartitioningRule)
            : base(rangePartitioningRule)
        {
        }

        public RangePartitioningRule(JsonObject jsonObject)
            : base(jsonObject)
        {
        }
    }

    public class RangePartitioningRule<TNumber> : RangePartitioningRule where TNumber : INumber<TNumber>
    {
        public RangePartitioningRule()
            : base()
        {
        }

        public RangePartitioningRule(RangePartitioningRule<TNumber> rangePartitioningRule)
            : base(rangePartitioningRule)
        {
        }

        public RangePartitioningRule(JsonObject jsonObject)
            : base(jsonObject)
        {
        }

        [JsonInclude, JsonPropertyName(nameof(Ranges))]
        public List<Range<TNumber>>? Ranges { get; set; }

        /// <summary>
        /// Converts a numeric value to a parametric string.
        /// Replaces '-' with 'm' at the start and '.' with 'p'.
        /// Example: -1.0010 becomes "m1p001".
        /// </summary>
        public static string ToString(TNumber value)
        {
            if (value is ISpanFormattable spanFormattable)
            {
                // Use stackalloc for initial formatting to determine the shape of the number
                Span<char> tempBuffer = stackalloc char[128];

                if (spanFormattable.TryFormat(tempBuffer, out int charsWritten, "G", CultureInfo.InvariantCulture))
                {
                    ReadOnlySpan<char> formattedSpan = tempBuffer.Slice(0, charsWritten);

                    bool isNegative = TNumber.IsNegative(value);
                    int dotIndex = formattedSpan.IndexOf('.');

                    // Calculate the final length:
                    // If negative, we replace '-' with 'm', so length stays the same.
                    int finalLength = charsWritten;

                    return string.Create(finalLength, (Value: value, IsNegative: isNegative, DotIndex: dotIndex), (Span<char> dest, (TNumber Value, bool IsNegative, int DotIndex) state) =>
                    {
                        // Re-format directly into the destination buffer
                        ((ISpanFormattable)state.Value).TryFormat(dest, out _, "G", CultureInfo.InvariantCulture);

                        // Handle Negative Sign: Replace '-' with 'm'
                        if (state.IsNegative)
                        {
                            // In InvariantCulture, the minus sign is always at index 0
                            dest[0] = 'm';
                        }

                        // Handle Decimal Separator: Replace '.' with 'p'
                        if (state.DotIndex != -1)
                        {
                            dest[state.DotIndex] = 'p';
                        }
                    });
                }
            }

            // Fallback for types not supporting ISpanFormattable
            string fallback = value.ToString("G", CultureInfo.InvariantCulture);
            if (TNumber.IsNegative(value))
            {
                fallback = "m" + fallback.Substring(1);
            }
            return fallback.Replace('.', 'p');
        }

        public string? GetPartitionSufix(TNumber value)
        {
            Range<TNumber>? range = Ranges?.Find(x => x.In(value));
            if (range is null)
            {
                return null;
            }

            string text_Min = ToString(range.Min);
            string text_Max = ToString(range.Max);

            return $"_{text_Min}_{text_Max}";
        }
    }
}