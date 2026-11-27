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
using Google.Api.Gax;

namespace Google.Cloud.Spanner.DataProvider;

/// <summary>
/// Represents a Cloud Spanner SQL dialect and provides dialect-specific identifier quoting.
/// </summary>
internal abstract class SpannerDialect
{
    /// <summary>
    /// All ASCII control characters (0x00-0x1F and 0x7F DEL).
    /// Subclasses combine this with dialect-specific delimiter/escape characters to build
    /// <see cref="SearchValues{Char}"/> sets for single-pass fast-path checks and validation.
    /// </summary>
    internal const string AsciiControlCharacterSet =
        "\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007" +
        "\u0008\u0009\u000A\u000B\u000C\u000D\u000E\u000F" +
        "\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017" +
        "\u0018\u0019\u001A\u001B\u001C\u001D\u001E\u001F\u007F";

    private static readonly SearchValues<char> AsciiControlCharacters = SearchValues.Create(AsciiControlCharacterSet);

    /// <summary>
    /// Singleton instance for the GoogleSQL dialect.
    /// </summary>
    internal static readonly SpannerDialect GoogleStandardSql = new GoogleStandardSqlDialect();

    /// <summary>
    /// Singleton instance for the PostgreSQL dialect.
    /// </summary>
    internal static readonly SpannerDialect Postgresql = new PostgresqlDialect();

    private protected SpannerDialect()
    {
    }

    /// <summary>
    /// Delimiter character used to quote identifiers in this dialect.
    /// </summary>
    internal abstract char IdentifierQuoteCharacter { get; }

    /// <summary>
    /// Characters disallowed on the single-segment identifier quoting fast path
    /// (ASCII control characters and dialect identifier quote/escape characters).
    /// </summary>
    protected abstract SearchValues<char> IdentifierControlOrEscapeCharacters { get; }

    /// <summary>
    /// Characters disallowed on the unquoted single-segment qualified identifier fast path
    /// (<see cref="IdentifierControlOrEscapeCharacters"/> plus '.').
    /// </summary>
    protected abstract SearchValues<char> IdentifierControlEscapeOrDotCharacters { get; }

    /// <summary>
    /// Quotes and escapes a possibly dot-qualified table identifier for this dialect.
    /// Already-quoted segments using the dialect's identifier quote delimiter are validated, unescaped, and re-quoted.
    /// For example, <c>"my_schema.my_table"</c> becomes <c>"`my_schema`.`my_table`"</c> (GoogleSQL) or
    /// <c>"\"my_schema\".\"my_table\""</c> (PostgreSQL), and <c>"`my.schema`.my_table"</c> preserves the dot inside the first segment.
    /// </summary>
    internal string QuoteQualifiedIdentifier(string identifier)
    {
        GaxPreconditions.CheckNotNullOrEmpty(identifier, nameof(identifier));

        var span = identifier.AsSpan();
        // Fast path: clean single unquoted segment (e.g. "my_table" -> "`my_table`").
        if (!span.ContainsAny(IdentifierControlEscapeOrDotCharacters))
        {
            return WrapInIdentifierQuotes(identifier);
        }

        // Fast path: already-quoted single segment with no interior escapes or control chars (e.g. "`my.table`").
        if (identifier.Length >= 3
            && identifier[0] == IdentifierQuoteCharacter
            && identifier[^1] == IdentifierQuoteCharacter
            && !span[1..^1].ContainsAny(IdentifierControlOrEscapeCharacters))
        {
            return identifier;
        }

        ValidateNoControlCharacters(span, nameof(identifier));

        var builder = new StringBuilder(identifier.Length + 8);
        var position = 0;
        var length = identifier.Length;

        while (position < length)
        {
            if (position > 0)
            {
                builder.Append('.');
            }

            // Quoted segments keep embedded dots inside a single identifier (e.g. "`my.table`"),
            // whereas unquoted segments terminate at the next '.'.
            if (identifier[position] == IdentifierQuoteCharacter)
            {
                position = ReadAndAppendQuotedIdentifierSegment(builder, identifier, position);
            }
            else
            {
                var dotIndex = identifier.IndexOf('.', position);
                var endIndex = dotIndex == -1 ? length : dotIndex;
                if (endIndex == position)
                {
                    throw new ArgumentException("Identifier segment cannot be empty.", nameof(identifier));
                }

                AppendQuotedIdentifierSegment(builder, span[position..endIndex]);
                position = endIndex;
            }

            if (position < length)
            {
                // Both segment branches guarantee that a non-EOF stopping position points to '.'.
                position++;
                if (position == length)
                {
                    throw new ArgumentException("Identifier cannot end with '.'.", nameof(identifier));
                }
            }
        }

        return builder.ToString();
    }

    /// <summary>
    /// Reads, validates, and appends a quoted identifier segment starting at <paramref name="startIndex"/>
    /// to <paramref name="builder"/>, returning the index immediately after its closing delimiter.
    /// </summary>
    protected abstract int ReadAndAppendQuotedIdentifierSegment(StringBuilder builder, string identifier, int startIndex);

    /// <summary>
    /// Appends a quoted and escaped identifier segment to <paramref name="builder"/>.
    /// </summary>
    protected abstract void AppendQuotedIdentifierSegment(StringBuilder builder, ReadOnlySpan<char> segment);

    /// <summary>
    /// Appends <paramref name="rawSegment"/> wrapped in <see cref="IdentifierQuoteCharacter"/> delimiters
    /// when its interior requires no further escaping.
    /// </summary>
    protected void AppendWrappedIdentifierSegment(StringBuilder builder, ReadOnlySpan<char> rawSegment)
    {
        builder.Append(IdentifierQuoteCharacter);
        builder.Append(rawSegment);
        builder.Append(IdentifierQuoteCharacter);
    }

    protected static bool IsAsciiControlCharacter(char character) =>
        character < ' ' || character == '\x7f';

    /// <summary>
    /// Ensures a closing identifier quote is followed either by end-of-string or a '.' qualifier separator,
    /// rejecting payloads with trailing SQL tokens such as <c>"`users` UNION SELECT 1"</c>.
    /// </summary>
    protected static void ValidateIdentifierSegmentTerminator(string identifier, int endIndex)
    {
        if (endIndex < identifier.Length && identifier[endIndex] != '.')
        {
            throw new ArgumentException("Unexpected character after quoted identifier.", nameof(identifier));
        }
    }

    private string WrapInIdentifierQuotes(string identifier) =>
        string.Create(identifier.Length + 2, (IdentifierQuoteCharacter, identifier), static (destination, state) =>
        {
            destination[0] = state.IdentifierQuoteCharacter;
            state.identifier.AsSpan().CopyTo(destination[1..]);
            destination[^1] = state.IdentifierQuoteCharacter;
        });

    internal static void ValidateNoControlCharacters(ReadOnlySpan<char> value, string parameterName)
    {
        if (value.ContainsAny(AsciiControlCharacters))
        {
            throw new ArgumentException("Identifier cannot contain ASCII control characters.", parameterName);
        }
    }
}
