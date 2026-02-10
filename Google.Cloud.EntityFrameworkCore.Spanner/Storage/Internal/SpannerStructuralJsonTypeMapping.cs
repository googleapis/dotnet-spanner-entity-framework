// Copyright 2025, Google LLC
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
using System.Data.Common;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;

/// <summary>
///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
///     the same compatibility standards as public APIs. It may be changed or removed without notice in
///     any release. You should only use it directly in your code with extreme caution and knowing that
///     doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
public class SpannerStructuralJsonTypeMapping : JsonTypeMapping
{
    private static readonly MethodInfo CreateUtf8StreamMethod
        = typeof(SpannerStructuralJsonTypeMapping).GetMethod(nameof(CreateUtf8Stream), [typeof(string)])!;

    private static readonly MethodInfo GetStringMethod
        = typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetString), [typeof(int)])!;

    public static SpannerStructuralJsonTypeMapping Default => JsonTypeDefault;

    public static SpannerStructuralJsonTypeMapping JsonTypeDefault { get; } = new("json");

    public SpannerStructuralJsonTypeMapping(string storeType)
        : base(storeType, typeof(JsonElement), System.Data.DbType.String)
    {
    }

    public override MethodInfo GetDataReaderMethod() => GetStringMethod;

    public static MemoryStream CreateUtf8Stream(string json)
        => json == ""
            ? throw new InvalidOperationException("json cannot be an empty string.")
            : new MemoryStream(Encoding.UTF8.GetBytes(json));

    public override Expression CustomizeDataReaderExpression(Expression expression)
        => Expression.Call(CreateUtf8StreamMethod, expression);

    protected SpannerStructuralJsonTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        if (value is string s)
        {
            return $"JSON '{s}'";
        }
        throw new ArgumentException($"{value} is not valid for database type JSON");
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new SpannerStructuralJsonTypeMapping(parameters);

    protected override void ConfigureParameter(DbParameter parameter)
    {
        ((SpannerParameter)parameter).SpannerDbType = SpannerDbType.Json;
        base.ConfigureParameter(parameter);
    }}