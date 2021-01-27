// Copyright 2021 Google LLC
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

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Linq.Expressions;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    internal class SpannerDateExtractExpression : SqlExpression
    {
        private readonly ISqlExpressionFactory _sqlExpressionFactory;
        private readonly string _dateTimePartName;
        private readonly SqlExpression _fromFragment;
        private readonly SqlExpression _value;

        internal SpannerDateExtractExpression(ISqlExpressionFactory sqlExpressionFactory, SqlExpression value, string dateTimePartName)
            : base(value.Type, value.TypeMapping)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
            _dateTimePartName = dateTimePartName;
            _value = value;
            _fromFragment = _sqlExpressionFactory.Fragment($"{_dateTimePartName} FROM ");
        }

        public override void Print(ExpressionPrinter expressionPrinter)
        {
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            visitor.Visit(_fromFragment);
            var newValue = visitor.Visit(_value);
            if (newValue != _value && newValue is SqlExpression newSqlValue)
            {
                return new SpannerDateExtractExpression(_sqlExpressionFactory, newSqlValue, _dateTimePartName);
            }
            return this;
        }

        public override bool Equals(object other)
        {
            if (other is SpannerDateExtractExpression o)
            {
                return _fromFragment.Equals(o._fromFragment) && _value.Equals(o._value);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash *= 31 + _fromFragment.GetHashCode();
            hash *= 31 + _value.GetHashCode();
            return hash;
        }
    }
}
