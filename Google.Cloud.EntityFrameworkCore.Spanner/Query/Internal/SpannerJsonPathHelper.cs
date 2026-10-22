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
            if (!NeedsQuotedNotation(propertyName))
            {
                return $".{propertyName}";
            }

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
                var escapedName = EscapeForRawSqlLiteral(propertyName);
                jsonPathBuilder.Append($".\"{escapedName}\"");
            }
            else
            {
                jsonPathBuilder.Append($".{propertyName}");
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
        /// Escapes special characters in a property name for use in a double-quoted JSONPath segment embedded
        /// within a raw Cloud Spanner SQL string literal.
        /// In GoogleSQL single-quoted string literals, backslashes must be doubled (\\) so they survive SQL lexical
        /// parsing as a single backslash in the JSONPath string seen by Spanner's JSON engine.
        /// Double quotes are escaped with \\\" so they are parsed as an escaped quote in JSONPath.
        /// Single quotes are escaped with \' so they do not terminate the enclosing SQL literal.
        /// </summary>
        private static string EscapeForRawSqlLiteral(string propertyName)
        {
            if (string.IsNullOrEmpty(propertyName))
            {
                return propertyName ?? string.Empty;
            }

            return propertyName
                .Replace("\\", "\\\\")      // Backslash -> 2 backslashes for SQL string literal
                .Replace("\"", "\\\\\"")    // Double quote -> 2 backslashes + quote (escaped at both levels)
                .Replace("'", "\\'")        // Single quote -> escaped for SQL string literal
                .Replace("\r", "\\\\r")
                .Replace("\n", "\\\\n")
                .Replace("\t", "\\\\t");
        }
    }
}
