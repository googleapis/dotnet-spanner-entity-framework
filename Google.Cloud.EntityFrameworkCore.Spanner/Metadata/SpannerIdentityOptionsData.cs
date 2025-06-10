// Copyright 2025, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Text;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Metadata;

public class SpannerIdentityOptionsData : IEquatable<SpannerIdentityOptionsData>
{
    public static readonly SpannerIdentityOptionsData Default = new ();

    public virtual GenerationStrategy GenerationStrategy { get; set; } = GenerationStrategy.GeneratedByDefault;

    [CanBeNull] public virtual string SequenceKind { get; set; } = "BIT_REVERSED_POSITIVE";

    public virtual string Serialize()
    {
        var builder = new StringBuilder();

        EscapeAndQuote(builder, GenerationStrategy);
        builder.Append(", ");
        EscapeAndQuote(builder, SequenceKind);

        return builder.ToString();
    }

    public static SpannerIdentityOptionsData Get(IReadOnlyAnnotatable annotatable)
        => Deserialize((string)annotatable[SpannerAnnotationNames.Identity]);

    public static SpannerIdentityOptionsData Deserialize(string value)
    {
        var data = new SpannerIdentityOptionsData();
        if (value is null)
        {
            return data;
        }
        try
        {
            var position = 0;
            data.GenerationStrategy = AsGenerationStrategy(ExtractValue(value, ref position)) ?? Default.GenerationStrategy;
            data.SequenceKind = ExtractValue(value, ref position) ?? Default.SequenceKind;
            return data;
        }
        catch (Exception ex)
        {
            throw new ArgumentException($"Couldn't deserialize {nameof(SpannerIdentityOptionsData)} from annotation", ex);
        }
    }

    private static void EscapeAndQuote(StringBuilder builder, object value)
    {
        builder.Append("'");
        if (value is not null)
        {
            builder.Append(value.ToString()!.Replace("'", "''"));
        }
        builder.Append("'");
    }

    private static string ExtractValue(string value, ref int position)
    {
        position = value.IndexOf('\'', position) + 1;

        var end = value.IndexOf('\'', position);

        while (end + 1 < value.Length
               && value[end + 1] == '\'')
        {
            end = value.IndexOf('\'', end + 2);
        }

        var extracted = value.Substring(position, end - position).Replace("''", "'");
        position = end + 1;

        return extracted.Length == 0 ? null : extracted;
    }

    private static GenerationStrategy? AsGenerationStrategy(string value)
    {
        if (value == null)
        {
            return null;
        }
        if (Enum.TryParse<GenerationStrategy>(value, out var generationStrategy))
        {
            return generationStrategy;
        }
        return null;
    }

    public override bool Equals(object obj)
        => obj is SpannerIdentityOptionsData other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(GenerationStrategy, SequenceKind);

    public virtual bool Equals(SpannerIdentityOptionsData other)
        => other is not null
           && (
               ReferenceEquals(this, other)
               || GenerationStrategy == other.GenerationStrategy
               && Equals(SequenceKind, other.SequenceKind)
           );
}