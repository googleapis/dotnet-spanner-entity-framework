// Copyright 2020, Google Inc. All rights reserved.
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

using Google.Api.Gax;
using Microsoft.EntityFrameworkCore.Storage;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// </summary>
    internal class SpannerSqlGenerationHelper : RelationalSqlGenerationHelper
    {
        //Note: This helper, used throughout SQL generation logic, holds provider specific settings such
        // as delimiters and statement terminators.

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public SpannerSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies, string statementTerminator = "")
            : base(dependencies)
        {
            Dependencies = dependencies;
            StatementTerminator = statementTerminator;
        }

        public new RelationalSqlGenerationHelperDependencies Dependencies { get; }

        // Spanner does not support multiple statements per query or in DDL. So in all cases, adding a
        // terminating semicolon just causes issues -- so we return an empty string for the statement
        // terminator.
        /// <inheritdoc />
        public override string StatementTerminator { get; }

        /// <inheritdoc />
        public override void GenerateParameterName(StringBuilder builder, string name)
        {
            builder.Append(GenerateParameterName(name));
        }

        private static bool NeedsEscaping(string identifier)
        {
            foreach (var character in identifier)
            {
                if (character < ' ' || character == '\x7f' || character == '\\' || character == '`')
                {
                    return true;
                }
            }
            return false;
        }

        /// <inheritdoc />
        public override void EscapeIdentifier(StringBuilder builder, string identifier)
        {
            GaxPreconditions.CheckNotNull(builder, nameof(builder));
            GaxPreconditions.CheckNotNullOrEmpty(identifier, nameof(identifier));

            if (!NeedsEscaping(identifier))
            {
                builder.Append(identifier);
                return;
            }

            foreach (var character in identifier)
            {
                switch (character)
                {
                    case '\\':
                        builder.Append(@"\\");
                        break;
                    case '`':
                        builder.Append(@"\`");
                        break;
                    case '\a':
                        builder.Append(@"\a");
                        break;
                    case '\b':
                        builder.Append(@"\b");
                        break;
                    case '\f':
                        builder.Append(@"\f");
                        break;
                    case '\n':
                        builder.Append(@"\n");
                        break;
                    case '\r':
                        builder.Append(@"\r");
                        break;
                    case '\t':
                        builder.Append(@"\t");
                        break;
                    case '\v':
                        builder.Append(@"\v");
                        break;
                    case < ' ':
                    case '\x7f':
                        builder.Append($@"\x{(int)character:x2}");
                        break;
                    default:
                        builder.Append(character);
                        break;
                }
            }
        }

        /// <inheritdoc />
        public override string EscapeIdentifier(string identifier)
        {
            GaxPreconditions.CheckNotNullOrEmpty(identifier, nameof(identifier));
            if (!NeedsEscaping(identifier))
            {
                return identifier;
            }

            var builder = new StringBuilder(identifier.Length + 4);
            EscapeIdentifier(builder, identifier);
            return builder.ToString();
        }

        // Note we remove the schema because spanner does not support schema based names.
        /// <inheritdoc />
        public override string DelimitIdentifier(string name, string schema)
            => DelimitIdentifier(name);

        /// <inheritdoc />
        public override void DelimitIdentifier(StringBuilder builder, string name, string schema)
            => DelimitIdentifier(builder, name);

        /// <inheritdoc />
        public override string DelimitIdentifier(string identifier)
            => $"`{EscapeIdentifier(identifier)}`";

        /// <inheritdoc />
        public override void DelimitIdentifier(StringBuilder builder, string identifier)
        {
            GaxPreconditions.CheckNotNull(builder, nameof(builder));
            GaxPreconditions.CheckNotNullOrEmpty(identifier, nameof(identifier));
            builder.Append('`');
            EscapeIdentifier(builder, identifier);
            builder.Append('`');
        }
    }
}
