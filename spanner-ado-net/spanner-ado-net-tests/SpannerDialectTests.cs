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
using Google.Cloud.Spanner.Admin.Database.V1;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class SpannerDialectTests
{
    private static SpannerDialect GetDialect(DatabaseDialect databaseDialect) => databaseDialect switch
    {
        DatabaseDialect.GoogleStandardSql => SpannerDialect.GoogleStandardSql,
        DatabaseDialect.Postgresql => SpannerDialect.Postgresql,
        _ => throw new ArgumentOutOfRangeException(nameof(databaseDialect), databaseDialect, null),
    };

    [TestCase("my_table", "`my_table`")]
    [TestCase("SELECT", "`SELECT`")]
    [TestCase("order", "`order`")]
    [TestCase("GROUP", "`GROUP`")]
    [TestCase("QUALIFY", "`QUALIFY`")]
    [TestCase("my-table", "`my-table`")]
    [TestCase("my table", "`my table`")]
    [TestCase("123table", "`123table`")]
    [TestCase("café", "`café`")]
    [TestCase("col_ñ", "`col_ñ`")]
    [TestCase("col/*comment*/name", "`col/*comment*/name`")]
    [TestCase("col;select", "`col;select`")]
    [TestCase("table`name", @"`table\`name`")]
    [TestCase(@"test\", @"`test\\`")]
    [TestCase(@"test\\", @"`test\\\\`")]
    [TestCase(@"test\`name", @"`test\\\`name`")]
    [TestCase(@"test\\`name", @"`test\\\\\`name`")]
    [TestCase("my\"table", "`my\"table`")]
    [TestCase("   ", "`   `")]
    [TestCase("\uff40", "`\uff40`")]
    [TestCase("-- c", "`-- c`")]
    [TestCase("/* c */", "`/* c */`")]
    [TestCase("# c", "`# c`")]
    [TestCase("col` # comment", @"`col\` # comment`")]
    [TestCase("col' OR '1'='1", "`col' OR '1'='1`")]
    [TestCase("col@p0", "`col@p0`")]
    [TestCase("col%s", "`col%s`")]
    [TestCase("a%%b", "`a%%b`")]
    [TestCase(" `abc`", @"` \`abc\``")]
    [TestCase("my_schema.my_table", "`my_schema`.`my_table`")]
    [TestCase("catalog.my_schema.my_table", "`catalog`.`my_schema`.`my_table`")]
    [TestCase("my-schema.my-table", "`my-schema`.`my-table`")]
    [TestCase("`my_table`", "`my_table`")]
    [TestCase("`my_schema`.`my_table`", "`my_schema`.`my_table`")]
    [TestCase("`my.table`", "`my.table`")]
    [TestCase("catalog.`my.schema`.my_table", "`catalog`.`my.schema`.`my_table`")]
    [TestCase("schema.table`name", @"`schema`.`table\`name`")]
    [TestCase("`my.schema`.my_table", "`my.schema`.`my_table`")]
    [TestCase("my_schema.`my.table`", "`my_schema`.`my.table`")]
    [TestCase(@"`my\u002etable`", "`my.table`")]
    [TestCase(@"`table\`name`", @"`table\`name`")]
    [TestCase(@"`table\\`", @"`table\\`")]
    [TestCase(@"`a\`b`", @"`a\`b`")]
    [TestCase(@"`a\` /**/ UNION SELECT 1 # `", @"`a\` /**/ UNION SELECT 1 # `")]
    [TestCase(@"`a\\nb`", @"`a\\nb`")]
    [TestCase(@"`a\\\`b`", @"`a\\\`b`")]
    [TestCase(@"`\?\""\'\`\\`", @"`?""'\`\\`")]
    [TestCase(@"`table\u0060name`", @"`table\`name`")]
    [TestCase(@"`\x60\140\U00000060`", @"`\`\`\``")]
    [TestCase(@"`\u005c\x5c\134`", @"`\\\\\\`")]
    [TestCase(@"`\x41\102\U00000043`", "`ABC`")]
    [TestCase(@"`\101\377`", "`A\u00ff`")]
    [TestCase(@"`\x41\X42\U0001f600`", "`AB\U0001f600`")]
    [TestCase(@"`\ud7ff\ue000\U0010ffff`", "`\ud7ff\ue000\U0010ffff`")]
    [TestCase(@"`my_schema`.`table\`name`", @"`my_schema`.`table\`name`")]
    [TestCase("users UNION SELECT * FROM secrets", "`users UNION SELECT * FROM secrets`")]
    [TestCase("users; DROP TABLE users; --", "`users; DROP TABLE users; --`")]
    [TestCase("users`; DROP TABLE users; --", @"`users\`; DROP TABLE users; --`")]
    [TestCase(@"users\`; DROP TABLE users; --", @"`users\\\`; DROP TABLE users; --`")]
    public void QuoteQualifiedIdentifier_GoogleSql_QuotesAndEscapesCorrectly(string input, string expected)
    {
        var quoted = SpannerDialect.GoogleStandardSql.QuoteQualifiedIdentifier(input);
        Assert.That(quoted, Is.EqualTo(expected));
        // Quoting must be idempotent for already-quoted outputs.
        Assert.That(SpannerDialect.GoogleStandardSql.QuoteQualifiedIdentifier(quoted), Is.EqualTo(expected));
    }

    [TestCase("my_table", "\"my_table\"")]
    [TestCase("select", "\"select\"")]
    [TestCase("order", "\"order\"")]
    [TestCase("user", "\"user\"")]
    [TestCase("my-table", "\"my-table\"")]
    [TestCase("my table", "\"my table\"")]
    [TestCase("   ", "\"   \"")]
    [TestCase("123table", "\"123table\"")]
    [TestCase("café", "\"café\"")]
    [TestCase("-- c", "\"-- c\"")]
    [TestCase("/* c */", "\"/* c */\"")]
    [TestCase("col' OR '1'='1", "\"col' OR '1'='1\"")]
    [TestCase("col@p0", "\"col@p0\"")]
    [TestCase("col$1", "\"col$1\"")]
    [TestCase("U&\"my_table\"", "\"U&\"\"my_table\"\"\"")]
    [TestCase(" \"abc\"", "\" \"\"abc\"\"\"")]
    [TestCase("table\"name", "\"table\"\"name\"")]
    [TestCase("schema.table\"name", "\"schema\".\"table\"\"name\"")]
    [TestCase(@"test\", "\"test\\\"")]
    [TestCase("\"table\\\"", "\"table\\\"")]
    [TestCase("test\\\"name", "\"test\\\"\"name\"")]
    [TestCase("table`name", "\"table`name\"")]
    [TestCase("public.my_table", "\"public\".\"my_table\"")]
    [TestCase("catalog.public.my_table", "\"catalog\".\"public\".\"my_table\"")]
    [TestCase("\"my_table\"", "\"my_table\"")]
    [TestCase("\"public\".\"my_table\"", "\"public\".\"my_table\"")]
    [TestCase("\"my.table\"", "\"my.table\"")]
    [TestCase("\"my.schema\".my_table", "\"my.schema\".\"my_table\"")]
    [TestCase("my_schema.\"my.table\"", "\"my_schema\".\"my.table\"")]
    [TestCase("\"table\"\"name\"", "\"table\"\"name\"")]
    [TestCase("\"\"\"a\"\"\"", "\"\"\"a\"\"\"")]
    [TestCase("\"\"\"a\"", "\"\"\"a\"")]
    [TestCase("\"\"\"\"", "\"\"\"\"")]
    [TestCase("\"a\"\"b\".\"c\"\"d\"", "\"a\"\"b\".\"c\"\"d\"")]
    [TestCase("users UNION SELECT * FROM secrets", "\"users UNION SELECT * FROM secrets\"")]
    [TestCase("users\"; DROP TABLE users; --", "\"users\"\"; DROP TABLE users; --\"")]
    public void QuoteQualifiedIdentifier_Postgresql_QuotesAndEscapesCorrectly(string input, string expected)
    {
        var quoted = SpannerDialect.Postgresql.QuoteQualifiedIdentifier(input);
        Assert.That(quoted, Is.EqualTo(expected));
        // Quoting must be idempotent for already-quoted outputs.
        Assert.That(SpannerDialect.Postgresql.QuoteQualifiedIdentifier(quoted), Is.EqualTo(expected));
    }

    [Test]
    public void QuoteQualifiedIdentifier_AllAsciiControlCharacters_ThrowsArgumentException(
        [Values(DatabaseDialect.GoogleStandardSql, DatabaseDialect.Postgresql)] DatabaseDialect databaseDialect)
    {
        var dialect = GetDialect(databaseDialect);
        for (var code = 0; code <= 0x1F; code++)
        {
            var identifier = $"table{(char)code}name";
            Assert.Throws<ArgumentException>(() => dialect.QuoteQualifiedIdentifier(identifier));
            Assert.Throws<ArgumentException>(() => dialect.QuoteQualifiedIdentifier($"{dialect.IdentifierQuoteCharacter}a{(char)code}b{dialect.IdentifierQuoteCharacter}"));
        }
        Assert.Throws<ArgumentException>(() => dialect.QuoteQualifiedIdentifier("table\x7fname"));
        Assert.Throws<ArgumentException>(() => dialect.QuoteQualifiedIdentifier("schema\n.table"));
        Assert.Throws<ArgumentException>(() => dialect.QuoteQualifiedIdentifier("schema.table\0"));
    }

    [TestCase(@"`table\aname`")]
    [TestCase(@"`table\bname`")]
    [TestCase(@"`table\fname`")]
    [TestCase(@"`table\nname`")]
    [TestCase(@"`table\rname`")]
    [TestCase(@"`table\tname`")]
    [TestCase(@"`table\vname`")]
    [TestCase(@"`table\000name`")]
    [TestCase(@"`table\177name`")]
    [TestCase(@"`table\x00name`")]
    [TestCase(@"`table\X00name`")]
    [TestCase(@"`table\x1fname`")]
    [TestCase(@"`table\x7fname`")]
    [TestCase(@"`table\u000aname`")]
    [TestCase(@"`table\u007fname`")]
    [TestCase(@"`table\U00000000name`")]
    [TestCase(@"`table\U0000007fname`")]
    public void QuoteQualifiedIdentifier_EscapedControlCharactersInGoogleSql_ThrowsArgumentException(string identifier)
    {
        Assert.Throws<ArgumentException>(() =>
            SpannerDialect.GoogleStandardSql.QuoteQualifiedIdentifier(identifier));
    }

    [TestCase(".")]
    [TestCase("..")]
    [TestCase("...")]
    [TestCase(".table")]
    [TestCase("schema.")]
    [TestCase("schema..table")]
    [TestCase("`schema`..`table`")]
    [TestCase("`")]
    [TestCase("``")]
    [TestCase("``.``")]
    [TestCase("``.`a`")]
    [TestCase("`a`.``")]
    [TestCase("schema.``")]
    [TestCase("`schema`.")]
    [TestCase("`my_table")]
    [TestCase(@"`my_table\`")]
    [TestCase(@"`a\\\`")]
    [TestCase(@"`a\\`b`")]
    [TestCase("schema.`my_table")]
    [TestCase("`a`.`b")]
    [TestCase("schema.`unclosed.table.`other")]
    [TestCase("`a`.`b` EXTRA")]
    [TestCase("`a.b`c.d")]
    [TestCase("`users` UNION SELECT `1`")]
    [TestCase("`users` UNION SELECT * FROM secrets --")]
    [TestCase("`users``other`")]
    [TestCase("```users```")]
    [TestCase(@"`a\`")]
    [TestCase(@"`a\z`")]
    [TestCase(@"`a\800`")]
    [TestCase(@"`a\0`")]
    [TestCase(@"`a\77`")]
    [TestCase(@"`a\01a`")]
    [TestCase(@"`a\400`")]
    [TestCase(@"`a\777`")]
    [TestCase(@"`a\12`")]
    [TestCase(@"`a\x`")]
    [TestCase(@"`a\x4`")]
    [TestCase(@"`a\x4z`")]
    [TestCase(@"`a\X4`")]
    [TestCase(@"`a\X4z`")]
    [TestCase(@"`a\u12`")]
    [TestCase(@"`a\u123`")]
    [TestCase(@"`a\u12zz`")]
    [TestCase(@"`a\uD800`")]
    [TestCase(@"`a\udfff`")]
    [TestCase(@"`a\U0000d800`")]
    [TestCase(@"`a\U00110000`")]
    [TestCase(@"`a\U80000000`")]
    [TestCase(@"`a\UFFFFFFFF`")]
    [TestCase(@"`a\U1234`")]
    [TestCase(@"`a\U1234567z`")]
    public void QuoteQualifiedIdentifier_InvalidGoogleSqlIdentifiers_ThrowsArgumentException(string identifier)
    {
        Assert.Throws<ArgumentException>(() =>
            SpannerDialect.GoogleStandardSql.QuoteQualifiedIdentifier(identifier));
    }

    [TestCase(".")]
    [TestCase("..")]
    [TestCase(".table")]
    [TestCase("schema.")]
    [TestCase("schema..table")]
    [TestCase("\"schema\"..\"table\"")]
    [TestCase("\"")]
    [TestCase("\"\"")]
    [TestCase("\"\".\"\"")]
    [TestCase("\"\".\"a\"")]
    [TestCase("\"a\".\"\"")]
    [TestCase("schema.\"\"")]
    [TestCase("\"schema\".")]
    [TestCase("\"my_table")]
    [TestCase("\"my_table\"\"other")]
    [TestCase("\"table\\\"other\"")]
    [TestCase("\"a\"\"\"b\"")]
    [TestCase("schema.\"my_table")]
    [TestCase("\"a\".\"b")]
    [TestCase("schema.\"unclosed.table.\"other")]
    [TestCase("\"a\".\"b\" EXTRA")]
    [TestCase("\"a.b\"c.d")]
    [TestCase("\"users\" UNION SELECT \"1\"")]
    [TestCase("\"users\" UNION SELECT * FROM secrets --")]
    [TestCase("\"users\" \"other\"")]
    public void QuoteQualifiedIdentifier_InvalidPostgresqlIdentifiers_ThrowsArgumentException(string identifier)
    {
        Assert.Throws<ArgumentException>(() =>
            SpannerDialect.Postgresql.QuoteQualifiedIdentifier(identifier));
    }

    [Test]
    public void QuoteQualifiedIdentifier_NullOrEmpty_Throws(
        [Values(DatabaseDialect.GoogleStandardSql, DatabaseDialect.Postgresql)] DatabaseDialect databaseDialect)
    {
        var dialect = GetDialect(databaseDialect);
        Assert.Throws<ArgumentNullException>(() => dialect.QuoteQualifiedIdentifier(null!));
        Assert.Throws<ArgumentException>(() => dialect.QuoteQualifiedIdentifier(string.Empty));
    }

    [Test]
    public void QuoteQualifiedIdentifier_CleanAlreadyQuotedSingleSegment_ReturnsOriginalInstance()
    {
        const string googleSqlQuoted = "`my_table`";
        const string googleSqlQuotedWithDot = "`my.table`";
        const string postgresqlQuoted = "\"my_table\"";
        const string postgresqlQuotedWithDot = "\"my.table\"";

        Assert.That(
            SpannerDialect.GoogleStandardSql.QuoteQualifiedIdentifier(googleSqlQuoted),
            Is.SameAs(googleSqlQuoted));
        Assert.That(
            SpannerDialect.GoogleStandardSql.QuoteQualifiedIdentifier(googleSqlQuotedWithDot),
            Is.SameAs(googleSqlQuotedWithDot));
        Assert.That(
            SpannerDialect.Postgresql.QuoteQualifiedIdentifier(postgresqlQuoted),
            Is.SameAs(postgresqlQuoted));
        Assert.That(
            SpannerDialect.Postgresql.QuoteQualifiedIdentifier(postgresqlQuotedWithDot),
            Is.SameAs(postgresqlQuotedWithDot));
    }
}
