// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Buffers;
using System.Text;

namespace Google.Cloud.Spanner.DataProvider;

/// <summary>
/// PostgreSQL dialect implementation for Cloud Spanner identifier quoting.
/// Quotes identifiers with double quotes (<c>"</c>) and escapes embedded double quotes by doubling them (<c>""</c>);
/// backslashes inside PostgreSQL identifiers are treated as literal characters.
/// </summary>
internal sealed class PostgresqlDialect : SpannerDialect
{
    private static readonly SearchValues<char> PostgresqlIdentifierControlOrEscapeCharacters =
        SearchValues.Create(AsciiControlCharacterSet + "\"");

    private static readonly SearchValues<char> PostgresqlIdentifierControlEscapeOrDotCharacters =
        SearchValues.Create(AsciiControlCharacterSet + "\".");

    internal PostgresqlDialect()
    {
    }

    internal override char IdentifierQuoteCharacter => '"';

    protected override SearchValues<char> IdentifierControlOrEscapeCharacters =>
        PostgresqlIdentifierControlOrEscapeCharacters;

    protected override SearchValues<char> IdentifierControlEscapeOrDotCharacters =>
        PostgresqlIdentifierControlEscapeOrDotCharacters;

    public override string ToString() => nameof(Postgresql);

    /// <summary>
    /// Reads and validates a double-quoted PostgreSQL segment starting at <paramref name="startIndex"/>
    /// and appends it to <paramref name="builder"/>.
    /// For example, <c>"\"a\"\"b\".c"</c> at index 0 appends <c>"\"a\"\"b\""</c> and returns 7 (pointing at <c>'.'</c>).
    /// </summary>
    protected override int ReadAndAppendQuotedIdentifierSegment(StringBuilder builder, string identifier, int startIndex)
    {
        var index = startIndex + 1;
        var length = identifier.Length;
        var contentStart = index;

        while (index < length)
        {
            var quoteIndex = identifier.IndexOf(IdentifierQuoteCharacter, index);
            if (quoteIndex == -1)
            {
                break;
            }

            // Two consecutive double quotes ("") represent a literal '"', not the end of the segment.
            if (quoteIndex + 1 < length && identifier[quoteIndex + 1] == IdentifierQuoteCharacter)
            {
                index = quoteIndex + 2;
                continue;
            }

            var endIndex = quoteIndex + 1;
            ValidateIdentifierSegmentTerminator(identifier, endIndex);
            if (quoteIndex == contentStart)
            {
                throw new ArgumentException("Identifier segment cannot be empty.", nameof(identifier));
            }

            // Since "" is already the canonical PostgreSQL escaped form and no unescaped quote
            // exists in [contentStart..quoteIndex], the validated slice can be copied directly.
            AppendWrappedIdentifierSegment(builder, identifier.AsSpan(contentStart, quoteIndex - contentStart));
            return endIndex;
        }

        throw new ArgumentException("Unterminated quoted identifier.", nameof(identifier));
    }

    /// <summary>
    /// Appends <paramref name="segment"/> wrapped in double quotes, escaping each embedded <c>"</c> as <c>""</c>.
    /// For example, <c>"table\"name"</c> appends <c>"\"table\"\"name\""</c>.
    /// </summary>
    protected override void AppendQuotedIdentifierSegment(StringBuilder builder, ReadOnlySpan<char> segment)
    {
        builder.Append(IdentifierQuoteCharacter);
        while (!segment.IsEmpty)
        {
            var quoteIndex = segment.IndexOf(IdentifierQuoteCharacter);
            if (quoteIndex == -1)
            {
                builder.Append(segment);
                break;
            }

            builder.Append(segment[..quoteIndex]);
            builder.Append("\"\"");
            segment = segment[(quoteIndex + 1)..];
        }
        builder.Append(IdentifierQuoteCharacter);
    }
}
