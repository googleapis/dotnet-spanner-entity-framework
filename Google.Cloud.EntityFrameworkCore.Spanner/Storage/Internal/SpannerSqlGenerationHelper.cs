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
using Microsoft.EntityFrameworkCore.Query;

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

        public override string GenerateParameterName(string name)
        {
            if (name.StartsWith(QueryCompilationContext.QueryParameterPrefix))
            {
                name = name[QueryCompilationContext.QueryParameterPrefix.Length..];
            }
            return base.GenerateParameterName(name);
        }

        /// <inheritdoc />
        public override void GenerateParameterName(StringBuilder builder, string name)
        {
            builder.Append(GenerateParameterName(name));
        }

        /// <inheritdoc />
        public override void EscapeIdentifier(StringBuilder builder, string identifier)
        {
            GaxPreconditions.CheckNotNullOrEmpty(identifier, nameof(identifier));
            var initialLength = builder.Length;
            builder.Append(identifier);
            builder.Replace("`", "\\`", initialLength, identifier.Length);
        }

        /// <inheritdoc />
        public override string EscapeIdentifier(string identifier)
        {
            GaxPreconditions.CheckNotNullOrEmpty(identifier, nameof(identifier));
            return identifier.Replace("`", "\\`");
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
            => $"`{EscapeIdentifier(GaxPreconditions.CheckNotNullOrEmpty(identifier, nameof(identifier)))}`";

        /// <inheritdoc />
        public override void DelimitIdentifier(StringBuilder builder, string identifier)
        {
            GaxPreconditions.CheckNotNullOrEmpty(identifier, nameof(identifier));
            builder.Append('`');
            EscapeIdentifier(builder, identifier);
            builder.Append('`');
        }
    }
}
