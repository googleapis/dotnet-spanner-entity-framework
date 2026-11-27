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
using System.Globalization;
using System.Text;

namespace Google.Cloud.Spanner.DataProvider;

/// <summary>
/// GoogleSQL dialect implementation for Cloud Spanner identifier quoting.
/// Quotes identifiers with backticks (<c>`</c>), escapes embedded backslashes (<c>\\</c>) and backticks (<c>\`</c>),
/// and validates/decodes GoogleSQL escape sequences inside already-quoted identifiers.
/// </summary>
internal sealed class GoogleStandardSqlDialect : SpannerDialect
{
    private static readonly SearchValues<char> GoogleSqlIdentifierControlOrEscapeCharacters =
        SearchValues.Create(AsciiControlCharacterSet + "`\\");

    private static readonly SearchValues<char> GoogleSqlIdentifierControlEscapeOrDotCharacters =
        SearchValues.Create(AsciiControlCharacterSet + "`\\.");

    internal GoogleStandardSqlDialect()
    {
    }

    internal override char IdentifierQuoteCharacter => '`';

    protected override SearchValues<char> IdentifierControlOrEscapeCharacters =>
        GoogleSqlIdentifierControlOrEscapeCharacters;

    protected override SearchValues<char> IdentifierControlEscapeOrDotCharacters =>
        GoogleSqlIdentifierControlEscapeOrDotCharacters;

    public override string ToString() => nameof(GoogleStandardSql);

    /// <summary>
    /// Reads a backtick-quoted GoogleSQL segment starting at <paramref name="startIndex"/>, decodes any backslash
    /// escapes, rejects escaped ASCII control characters, and appends the canonically quoted segment to <paramref name="builder"/>.
    /// For example, <c>"`my.schema`.my_table"</c> at index 0 appends <c>"`my.schema`"</c> and returns 11 (pointing at <c>'.'</c>),
    /// and <c>"`table\u0060name`"</c> is normalized to <c>"`table\`name`"</c>.
    /// </summary>
    protected override int ReadAndAppendQuotedIdentifierSegment(StringBuilder builder, string identifier, int startIndex)
    {
        var contentStart = startIndex + 1;
        var span = identifier.AsSpan(contentStart);
        var specialOffset = span.IndexOfAny(IdentifierQuoteCharacter, '\\');
        if (specialOffset == -1)
        {
            throw new ArgumentException("Unterminated quoted identifier.", nameof(identifier));
        }

        // Fast path: no backslash escapes in this quoted segment (e.g. "`my.schema`").
        if (span[specialOffset] == IdentifierQuoteCharacter)
        {
            if (specialOffset == 0)
            {
                throw new ArgumentException("Identifier segment cannot be empty.", nameof(identifier));
            }

            var endIndex = contentStart + specialOffset + 1;
            ValidateIdentifierSegmentTerminator(identifier, endIndex);
            AppendWrappedIdentifierSegment(builder, span[..specialOffset]);
            return endIndex;
        }

        // Backslash escape path: copy the unescaped prefix before the first '\', then decode escapes
        // in a single pass from left to right so adjacent escapes cannot interfere with each other.
        // For example:
        //   - `a\\nb`   -> '\\' decodes to literal '\' and 'n' stays 'n' (decoded: a\nb -> re-quoted: `a\\nb`),
        //                  so it is NOT misread as `a\` followed by a newline escape '\n'.
        //   - `a\`b`    -> '\`' decodes to literal '`' instead of closing the quoted segment
        //                  (decoded: a`b -> re-quoted: `a\`b`).
        //   - `a\u000a` -> decodes to LF (0x0A), which is then caught and rejected as a control character.
        var segmentBuilder = new StringBuilder();
        segmentBuilder.Append(span[..specialOffset]);
        var index = contentStart + specialOffset;
        var length = identifier.Length;

        while (index < length)
        {
            var character = identifier[index];
            if (character == '\\')
            {
                var (decoded, nextIndex) = DecodeIdentifierEscape(identifier, index);
                foreach (var decodedCharacter in decoded)
                {
                    // Reject escaped control characters (e.g. "\n", "\x00", "\u000a") just like raw ones.
                    if (IsAsciiControlCharacter(decodedCharacter))
                    {
                        throw new ArgumentException("Identifier cannot contain ASCII control characters.", nameof(identifier));
                    }
                    segmentBuilder.Append(decodedCharacter);
                }
                index = nextIndex;
                continue;
            }

            if (character == IdentifierQuoteCharacter)
            {
                var endIndex = index + 1;
                ValidateIdentifierSegmentTerminator(identifier, endIndex);
                AppendQuotedIdentifierSegment(builder, segmentBuilder.ToString().AsSpan());
                return endIndex;
            }

            segmentBuilder.Append(character);
            index++;
        }

        throw new ArgumentException("Unterminated quoted identifier.", nameof(identifier));
    }

    /// <summary>
    /// Appends <paramref name="segment"/> wrapped in backticks, escaping <c>\</c> as <c>\\</c> and <c>`</c> as <c>\`</c>.
    /// For example, <c>"table`name"</c> appends <c>"`table\`name`"</c> and <c>"test\\"</c> appends <c>"`test\\\\`"</c>.
    /// </summary>
    protected override void AppendQuotedIdentifierSegment(StringBuilder builder, ReadOnlySpan<char> segment)
    {
        builder.Append(IdentifierQuoteCharacter);
        while (!segment.IsEmpty)
        {
            var escapeIndex = segment.IndexOfAny('\\', IdentifierQuoteCharacter);
            if (escapeIndex == -1)
            {
                builder.Append(segment);
                break;
            }

            builder.Append(segment[..escapeIndex]);
            builder.Append(segment[escapeIndex] == '\\' ? @"\\" : @"\`");
            segment = segment[(escapeIndex + 1)..];
        }
        builder.Append(IdentifierQuoteCharacter);
    }

    /// <summary>
    /// Decodes one GoogleSQL backslash escape sequence starting at <paramref name="backslashIndex"/>,
    /// returning the decoded string and the index immediately after the escape sequence.
    /// For example, decoding <c>"\n"</c> at index 1 of <c>"`a\nb`"</c> returns <c>("\n", 3)</c>.
    /// </summary>
    private static (string Decoded, int NextIndex) DecodeIdentifierEscape(string identifier, int backslashIndex)
    {
        var escapeStartIndex = backslashIndex + 1;
        if (escapeStartIndex >= identifier.Length)
        {
            throw new ArgumentException("Unterminated escape sequence in identifier.", nameof(identifier));
        }

        var escapeCharacter = identifier[escapeStartIndex];
        switch (escapeCharacter)
        {
            case 'a':
                return ("\a", escapeStartIndex + 1);
            case 'b':
                return ("\b", escapeStartIndex + 1);
            case 'f':
                return ("\f", escapeStartIndex + 1);
            case 'n':
                return ("\n", escapeStartIndex + 1);
            case 'r':
                return ("\r", escapeStartIndex + 1);
            case 't':
                return ("\t", escapeStartIndex + 1);
            case 'v':
                return ("\v", escapeStartIndex + 1);
            case '\\':
                return ("\\", escapeStartIndex + 1);
            case '?':
                return ("?", escapeStartIndex + 1);
            case '"':
                return ("\"", escapeStartIndex + 1);
            case '\'':
                return ("'", escapeStartIndex + 1);
            case '`':
                return ("`", escapeStartIndex + 1);
            case >= '0' and <= '7':
                return DecodeOctalEscape(identifier, escapeStartIndex);
            case 'x':
            case 'X':
                return DecodeHexEscape(identifier, escapeStartIndex, digitCount: 2, checkUnicodeScalar: false);
            case 'u':
                return DecodeHexEscape(identifier, escapeStartIndex, digitCount: 4, checkUnicodeScalar: true);
            case 'U':
                return DecodeHexEscape(identifier, escapeStartIndex, digitCount: 8, checkUnicodeScalar: true);
            default:
                throw new ArgumentException($"Invalid escape sequence in identifier: '\\{escapeCharacter}'.", nameof(identifier));
        }
    }

    /// <summary>
    /// Decodes a fixed 3-digit GoogleSQL octal escape (<c>\ooo</c>, 0x00-0xFF) starting at the first octal digit.
    /// For example, <c>"\101"</c> at <paramref name="escapeStartIndex"/>=2 in <c>"`a\101b`"</c> returns <c>("A", 5)</c>.
    /// </summary>
    private static (string Decoded, int NextIndex) DecodeOctalEscape(string identifier, int escapeStartIndex)
    {
        const int digitCount = 3;
        var endIndex = escapeStartIndex + digitCount;
        if (endIndex > identifier.Length)
        {
            throw new ArgumentException("Invalid octal escape sequence in identifier.", nameof(identifier));
        }

        var value = 0;
        for (var index = escapeStartIndex; index < endIndex; index++)
        {
            var digit = identifier[index];
            if (digit is < '0' or > '7')
            {
                throw new ArgumentException("Invalid octal escape sequence in identifier.", nameof(identifier));
            }
            value = (value * 8) + (digit - '0');
        }

        if (value > 0xFF)
        {
            throw new ArgumentException("Octal escape sequence in identifier exceeds 0xFF.", nameof(identifier));
        }

        return (((char)value).ToString(), endIndex);
    }

    /// <summary>
    /// Decodes a fixed-width GoogleSQL hex or Unicode escape (<c>\xHH</c>, <c>\XHH</c>, <c>\uHHHH</c>, or <c>\UHHHHHHHH</c>).
    /// When <paramref name="checkUnicodeScalar"/> is true, rejects UTF-16 surrogates (U+D800-U+DFFF) and values above U+10FFFF.
    /// For example, <c>"\u0060"</c> returns <c>("`", ...)</c>, and <c>"\U0001f600"</c> returns <c>("😀", ...)</c>.
    /// </summary>
    private static (string Decoded, int NextIndex) DecodeHexEscape(
        string identifier, int escapeStartIndex, int digitCount, bool checkUnicodeScalar)
    {
        var digitsStartIndex = escapeStartIndex + 1;
        var endIndex = digitsStartIndex + digitCount;
        if (endIndex > identifier.Length)
        {
            throw new ArgumentException($"Invalid '\\{identifier[escapeStartIndex]}' escape sequence in identifier.", nameof(identifier));
        }

        var hexDigits = identifier.AsSpan(digitsStartIndex, digitCount);
        foreach (var digit in hexDigits)
        {
            if (!char.IsAsciiHexDigit(digit))
            {
                throw new ArgumentException($"Invalid '\\{identifier[escapeStartIndex]}' escape sequence in identifier.", nameof(identifier));
            }
        }

        // Parse as uint so 8-digit values >= 0x80000000 (e.g. \UFFFFFFFF) are caught by > 0x10FFFF instead of wrapping negative.
        var codepoint = uint.Parse(hexDigits, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture);
        if (checkUnicodeScalar)
        {
            if (codepoint is (>= 0xD800 and <= 0xDFFF) or > 0x10FFFF)
            {
                throw new ArgumentException($"Invalid Unicode codepoint in identifier: U+{codepoint:X4}.", nameof(identifier));
            }
            return (char.ConvertFromUtf32((int)codepoint), endIndex);
        }

        return (((char)codepoint).ToString(), endIndex);
    }
}
