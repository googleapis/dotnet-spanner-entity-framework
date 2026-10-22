// Copyright 2022 Google LLC
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

#nullable enable

using System;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

using Type = System.Type;

/// <summary>
/// Only for internal use.
/// </summary>
public class SpannerSqlExpressionFactory : SqlExpressionFactory
{
    private readonly RelationalTypeMapping _boolTypeMapping;

    public SpannerSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies) : base(dependencies)
    {
        _boolTypeMapping = dependencies.TypeMappingSource.FindMapping(typeof(bool), dependencies.Model)!;
    }

    public override SqlExpression Convert(SqlExpression operand, Type type, RelationalTypeMapping? typeMapping = null)
    {
        typeMapping ??= Dependencies.TypeMappingSource.FindMapping(type, Dependencies.Model);
        return base.Convert(operand, type, typeMapping);
    }

    public override SqlExpression In(SqlExpression item, SqlParameterExpression valuesParameter)
    {
        var itemTypeMapping = item.TypeMapping ?? Dependencies.TypeMappingSource.FindMapping(item.Type, Dependencies.Model);
        var parametersTypeMapping = valuesParameter.TypeMapping
            ?? Dependencies.TypeMappingSource.FindMapping(valuesParameter.Type, Dependencies.Model, itemTypeMapping)
            ?? Dependencies.TypeMappingSource.FindMapping(valuesParameter.Type, Dependencies.Model);

        // TODO: Clean this up once we have modernized our type mapping definitions to consistently populate ElementTypeMapping.
        if (itemTypeMapping != null
            && parametersTypeMapping != null
            && (parametersTypeMapping.ElementTypeMapping != null
                || parametersTypeMapping.StoreType.StartsWith("ARRAY", StringComparison.OrdinalIgnoreCase)))
        {
            item = ApplyTypeMapping(item, itemTypeMapping);
            valuesParameter = valuesParameter.ApplyTypeMapping(parametersTypeMapping);

            return new SpannerInExpression(
                item,
                valuesParameter,
                _boolTypeMapping);
        }

        return base.In(item, valuesParameter);
    }

    public virtual SpannerContainsExpression SpannerContains(SqlExpression item, SqlExpression values, bool negated)
    {
        var typeMapping = item.TypeMapping ?? Dependencies.TypeMappingSource.FindMapping(item.Type, Dependencies.Model);

        item = ApplyTypeMapping(item, typeMapping);
        values = ApplyTypeMapping(values, typeMapping);

        return new SpannerContainsExpression(item, values, negated, _boolTypeMapping);
    }
}
