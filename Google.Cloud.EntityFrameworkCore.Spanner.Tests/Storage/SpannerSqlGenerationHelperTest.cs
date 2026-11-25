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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Linq;
using System.Text;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Storage
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class SpannerSqlGenerationHelperTest
    {
        [Theory]
        [InlineData("Singers", "Singers")]
        [InlineData(" ", " ")]
        [InlineData("~", "~")]
        [InlineData("\x80", "\x80")]
        [InlineData("Café", "Café")]
        [InlineData("🎵", "🎵")]
        [InlineData("`", @"\`")]
        [InlineData(@"\", @"\\")]
        [InlineData("\0", @"\x00")]
        [InlineData("My`Table", @"My\`Table")]
        [InlineData(@"My\Table", @"My\\Table")]
        [InlineData(@"TrailingBackslash\", @"TrailingBackslash\\")]
        [InlineData(@"foo\` SELECT 1 --", @"foo\\\` SELECT 1 --")]
        [InlineData(@"\\`\\`", @"\\\\\`\\\\\`")]
        [InlineData("Line1\nLine2\r\nLine3", @"Line1\nLine2\r\nLine3")]
        [InlineData("Col\a\b\f\t\vName", @"Col\a\b\f\t\vName")]
        [InlineData("Null\0Char", @"Null\x00Char")]
        [InlineData(@"Café`\🎵", @"Café\`\\🎵")]
        public void EscapeIdentifier_SpecialCharacters_EscapesCorrectly(string identifier, string expected)
        {
            using var context = SpannerTestHelpers.Instance.CreateContext();
            var helper = Assert.IsType<SpannerSqlGenerationHelper>(context.GetService<ISqlGenerationHelper>());

            var actual = helper.EscapeIdentifier(identifier);
            Assert.Equal(expected, actual);
            if (identifier == expected)
            {
                Assert.Same(identifier, actual);
            }

            var builder = new StringBuilder("SELECT ");
            helper.EscapeIdentifier(builder, identifier);
            Assert.Equal($"SELECT {expected}", builder.ToString());
        }

        [Fact]
        public void EscapeIdentifier_AllAsciiControlCharacters_EscapesCorrectly()
        {
            using var context = SpannerTestHelpers.Instance.CreateContext();
            var helper = Assert.IsType<SpannerSqlGenerationHelper>(context.GetService<ISqlGenerationHelper>());

            foreach (var code in Enumerable.Range(0, 0x20).Append(0x7F))
            {
                var character = (char)code;
                var expectedEscape = character switch
                {
                    '\a' => @"\a",
                    '\b' => @"\b",
                    '\f' => @"\f",
                    '\n' => @"\n",
                    '\r' => @"\r",
                    '\t' => @"\t",
                    '\v' => @"\v",
                    _ => $@"\x{code:x2}",
                };

                var identifier = $"col{character}name";
                var expected = $"col{expectedEscape}name";
                Assert.Equal(expected, helper.EscapeIdentifier(identifier));

                var builder = new StringBuilder();
                helper.EscapeIdentifier(builder, identifier);
                Assert.Equal(expected, builder.ToString());
            }
        }

        [Theory]
        [InlineData("Singers", "`Singers`")]
        [InlineData("Café", "`Café`")]
        [InlineData(@"Café`\🎵", @"`Café\`\\🎵`")]
        [InlineData(@"TrailingBackslash\", @"`TrailingBackslash\\`")]
        [InlineData(@"foo\` SELECT 1 --", @"`foo\\\` SELECT 1 --`")]
        [InlineData("Line1\nLine2", @"`Line1\nLine2`")]
        [InlineData("Null\0Char", @"`Null\x00Char`")]
        [InlineData("Del\u007fChar", @"`Del\x7fChar`")]
        public void DelimitIdentifier_ValidIdentifier_EscapesAndQuotes(string identifier, string expected)
        {
            using var context = SpannerTestHelpers.Instance.CreateContext();
            var helper = Assert.IsType<SpannerSqlGenerationHelper>(context.GetService<ISqlGenerationHelper>());

            Assert.Equal(expected, helper.DelimitIdentifier(identifier));
            Assert.Equal(expected, helper.DelimitIdentifier(identifier, null));
            Assert.Equal(expected, helper.DelimitIdentifier(identifier, string.Empty));
            Assert.Equal(expected, helper.DelimitIdentifier(identifier, "ignored_schema"));
            Assert.Equal(expected, helper.DelimitIdentifier(identifier, "bad`schema"));

            var builder = new StringBuilder("FROM ");
            helper.DelimitIdentifier(builder, identifier);
            Assert.Equal($"FROM {expected}", builder.ToString());

            var nullSchemaBuilder = new StringBuilder("FROM ");
            helper.DelimitIdentifier(nullSchemaBuilder, identifier, null);
            Assert.Equal($"FROM {expected}", nullSchemaBuilder.ToString());

            var emptySchemaBuilder = new StringBuilder("FROM ");
            helper.DelimitIdentifier(emptySchemaBuilder, identifier, string.Empty);
            Assert.Equal($"FROM {expected}", emptySchemaBuilder.ToString());

            var schemaBuilder = new StringBuilder("FROM ");
            helper.DelimitIdentifier(schemaBuilder, identifier, "bad`schema");
            Assert.Equal($"FROM {expected}", schemaBuilder.ToString());
        }

        [Fact]
        public void EscapeIdentifier_NullOrEmpty_Throws()
        {
            using var context = SpannerTestHelpers.Instance.CreateContext();
            var helper = Assert.IsType<SpannerSqlGenerationHelper>(context.GetService<ISqlGenerationHelper>());

            Assert.Throws<ArgumentNullException>(() => helper.EscapeIdentifier(null));
            Assert.Throws<ArgumentException>(() => helper.EscapeIdentifier(string.Empty));
            Assert.Throws<ArgumentNullException>(() => helper.EscapeIdentifier(null, "Singers"));
            Assert.Throws<ArgumentNullException>(() => helper.EscapeIdentifier(new StringBuilder(), null));
            Assert.Throws<ArgumentException>(() => helper.EscapeIdentifier(new StringBuilder(), string.Empty));
        }

        [Fact]
        public void DelimitIdentifier_NullOrEmpty_Throws()
        {
            using var context = SpannerTestHelpers.Instance.CreateContext();
            var helper = Assert.IsType<SpannerSqlGenerationHelper>(context.GetService<ISqlGenerationHelper>());

            Assert.Throws<ArgumentNullException>(() => helper.DelimitIdentifier(null));
            Assert.Throws<ArgumentException>(() => helper.DelimitIdentifier(string.Empty));
            Assert.Throws<ArgumentNullException>(() => helper.DelimitIdentifier((string)null, "ignored_schema"));
            Assert.Throws<ArgumentException>(() => helper.DelimitIdentifier(string.Empty, "ignored_schema"));
            Assert.Throws<ArgumentNullException>(() => helper.DelimitIdentifier((StringBuilder)null, "Singers"));
            Assert.Throws<ArgumentNullException>(() => helper.DelimitIdentifier((StringBuilder)null, "Singers", "ignored_schema"));
            Assert.Throws<ArgumentNullException>(() => helper.DelimitIdentifier(new StringBuilder(), null));
            Assert.Throws<ArgumentException>(() => helper.DelimitIdentifier(new StringBuilder(), string.Empty));
            Assert.Throws<ArgumentNullException>(() => helper.DelimitIdentifier(new StringBuilder(), null, "ignored_schema"));
            Assert.Throws<ArgumentException>(() => helper.DelimitIdentifier(new StringBuilder(), string.Empty, "ignored_schema"));
        }
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
