// Copyright 2026 Google LLC
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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    /// <summary>
    /// Helper methods for constructing and formatting Cloud Spanner JSONPath expressions
    /// used in JSON_VALUE and JSON_QUERY SQL functions.
    /// </summary>
    internal static class SpannerJsonPathHelper
    {
        /// <summary>
        /// Builds a Cloud Spanner JSONPath expression (e.g. "$.Property.SubProperty[0]") from EF Core path segments.
        /// </summary>
        internal static string BuildJsonPath(IReadOnlyList<PathSegment> pathSegments)
        {
            var jsonPathBuilder = new StringBuilder("$");
            foreach (var pathSegment in pathSegments)
            {
                if (pathSegment.PropertyName != null)
                {
                    AppendPropertySegment(jsonPathBuilder, pathSegment.PropertyName);
                }
                else if (pathSegment.ArrayIndex != null)
                {
                    AppendArrayIndexSegment(jsonPathBuilder, pathSegment.ArrayIndex);
                }
            }

            return jsonPathBuilder.ToString();
        }

        /// <summary>
        /// Determines whether a JSON property name requires double-quoted notation ($."...") in Cloud Spanner JSONPath.
        /// Names that start with an ASCII letter or underscore and contain only ASCII alphanumeric characters and
        /// underscores can use standard dot notation (e.g. $.propertyName). Any other characters (dots, spaces,
        /// quotes, backslashes, non-ASCII characters) require quoted notation (e.g. $."property.with.dot", $."prénom").
        /// </summary>
        internal static bool NeedsQuotedNotation(string propertyName)
        {
            if (string.IsNullOrEmpty(propertyName))
            {
                return true;
            }

            if (!char.IsAsciiLetter(propertyName[0]) && propertyName[0] != '_')
            {
                return true;
            }

            for (var index = 1; index < propertyName.Length; index++)
            {
                var character = propertyName[index];
                if (!char.IsAsciiLetterOrDigit(character) && character != '_')
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Formats a property segment for use in a JSONPath constant expression (e.g. for SqlConstantExpression in SpannerJsonMethodCallTranslator).
        /// Standard alphanumeric identifiers use dot notation (e.g. ".PropertyName").
        /// Names containing special characters use double-quoted notation with escaped double quotes (e.g. ".\"property.name\"").
        /// Does NOT double backslashes because SpannerStringTypeMapping handles SQL string literal escaping for constants.
        /// </summary>
        internal static string FormatPropertySegmentForConstant(string propertyName)
        {
            ArgumentNullException.ThrowIfNull(propertyName);

            if (!NeedsQuotedNotation(propertyName))
            {
                return $".{propertyName}";
            }

            ValidateQuotedPropertyName(propertyName);
            var escapedName = propertyName.Replace("\"", "\\\"");
            return $".\"{escapedName}\"";
        }

        /// <summary>
        /// Appends a JSON property name segment to the current JSONPath literal builder for raw SQL literal emission.
        /// Standard alphanumeric identifiers use dot notation (e.g. ".PropertyName").
        /// Names containing special characters use double-quoted notation with escaped characters (e.g. ".\"property.name\"").
        /// </summary>
        private static void AppendPropertySegment(StringBuilder jsonPathBuilder, string propertyName)
        {
            if (NeedsQuotedNotation(propertyName))
            {
                ValidateQuotedPropertyName(propertyName);
                jsonPathBuilder.Append(".\"");
                AppendEscapedForRawSqlLiteral(jsonPathBuilder, propertyName);
                jsonPathBuilder.Append('"');
            }
            else
            {
                jsonPathBuilder.Append('.').Append(propertyName);
            }
        }

        /// <summary>
        /// Rejects property names that cannot be represented in Cloud Spanner's double-quoted JSONPath
        /// syntax ($."..."). In Spanner JSONPath, \" escapes a quote, while \\ is kept as two backslashes.
        /// Because of that, two patterns break the closing quote of the segment:
        /// - Ending with an odd number of backslashes (e.g. foo\ -> $."foo\" leaves the closing quote escaped).
        /// - A double quote preceded by an odd number of backslashes (e.g. a\"b -> escaping " produces $."a\\"b",
        ///   where the even backslash count causes " to close the segment early).
        /// </summary>
        private static void ValidateQuotedPropertyName(string propertyName)
        {
            ArgumentNullException.ThrowIfNull(propertyName);
            var consecutiveBackslashes = 0;
            foreach (var character in propertyName)
            {
                if (character == '\\')
                {
                    consecutiveBackslashes++;
                }
                else
                {
                    if (character == '"' && (consecutiveBackslashes & 1) == 1)
                    {
                        throw new NotSupportedException(
                            "Cloud Spanner JSONPath does not support JSON property names containing an odd number of backslashes immediately before a double quote.");
                    }
                    consecutiveBackslashes = 0;
                }
            }

            if ((consecutiveBackslashes & 1) == 1)
            {
                throw new NotSupportedException(
                    "Cloud Spanner JSONPath does not support JSON property names ending with an odd number of backslashes.");
            }
        }

        /// <summary>
        /// Appends a JSON array index segment to the JSONPath builder.
        /// Constant indices are appended directly to the literal builder (e.g. "[0]").
        /// Non-constant indices (dynamic expressions or parameters) cannot be evaluated dynamically
        /// because Cloud Spanner requires argument 2 of JSON_VALUE and JSON_QUERY to be a literal or query parameter.
        /// </summary>
        private static void AppendArrayIndexSegment(StringBuilder jsonPathBuilder, Expression arrayIndexExpression)
        {
            if (arrayIndexExpression is SqlConstantExpression { Value: int or long } constantIndex)
            {
                jsonPathBuilder.Append($"[{constantIndex.Value}]");
            }
            else
            {
                throw new NotSupportedException(
                    "The Cloud Spanner EF Core provider does not support using a non-constant index when querying a JSON array.");
            }
        }

        /// <summary>
        /// Appends a property name escaped for a double-quoted JSONPath segment ($."...") inside a
        /// single-quoted SQL literal ('...'). Escaping accounts for two parsing passes (SQL first, then JSONPath):
        /// - \  -> \\  (SQL unescapes to \, which JSONPath receives literally)
        /// - "  -> \\" (SQL unescapes to \", which JSONPath treats as an escaped ")
        /// - '  -> \'  (escaped for the outer SQL single-quoted literal)
        /// - Control characters -> \a, \b, \f, \n, \r, \t, \v, or \xHH (SQL unescapes back to the exact char)
        /// </summary>
        private static void AppendEscapedForRawSqlLiteral(StringBuilder builder, string propertyName)
        {
            foreach (var character in propertyName)
            {
                switch (character)
                {
                    case '\\':
                        builder.Append(@"\\");
                        break;
                    case '"':
                        builder.Append("\\\\\"");
                        break;
                    case '\'':
                        builder.Append(@"\'");
                        break;
                    default:
                        if (!SpannerSqlGenerationHelper.TryAppendEscapedControlCharacter(builder, character))
                        {
                            builder.Append(character);
                        }
                        break;
                }
            }
        }
    }
}
