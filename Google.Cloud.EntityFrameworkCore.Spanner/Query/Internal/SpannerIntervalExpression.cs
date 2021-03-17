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
    internal class SpannerIntervalExpression : SqlExpression
    {
        private readonly ISqlExpressionFactory _sqlExpressionFactory;
        private readonly string _intervalName;
        private readonly SqlExpression _intervalFragment;
        private readonly SqlExpression _intervalNameFragment;
        private SqlExpression _value;

        internal SpannerIntervalExpression(ISqlExpressionFactory sqlExpressionFactory, SqlExpression value, string intervalName) : base(value.Type, value.TypeMapping)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
            _intervalName = intervalName;
            _value = value;
            _intervalFragment = _sqlExpressionFactory.Fragment("INTERVAL ");
            _intervalNameFragment = _sqlExpressionFactory.Fragment($" {_intervalName}");
        }

        public override void Print(ExpressionPrinter expressionPrinter)
        {
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            visitor.Visit(_intervalFragment);
            var newValue = visitor.Visit(_value);
            visitor.Visit(_intervalNameFragment);
            if (newValue != _value && newValue is SqlExpression newSqlValue)
            {
                return new SpannerIntervalExpression(_sqlExpressionFactory, newSqlValue, _intervalName);
            }
            return this;
        }

        public override bool Equals(object other)
        {
            if (other is SpannerIntervalExpression o)
            {
                return _intervalFragment.Equals(o._intervalFragment) && _value.Equals(o._value) && _intervalNameFragment.Equals(o._intervalNameFragment);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash *= 31 + _intervalFragment.GetHashCode();
            hash *= 31 + _value.GetHashCode();
            hash *= _intervalNameFragment.GetHashCode();
            return hash;
        }
    }
}
