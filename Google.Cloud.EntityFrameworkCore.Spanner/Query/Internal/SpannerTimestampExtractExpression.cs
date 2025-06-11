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
    internal class SpannerTimestampExtractExpression : SpannerValueExpression
    {
        private readonly ISqlExpressionFactory _sqlExpressionFactory;
        private readonly string _dateTimePartName;
        private readonly SqlExpression _fromFragment;
        private readonly SqlExpression _timezoneFragment;
        private readonly SqlExpression _value;

        internal override SqlExpression Value => _value;

        internal SpannerTimestampExtractExpression(ISqlExpressionFactory sqlExpressionFactory, SqlExpression value, string dateTimePartName) : base(value.Type, value.TypeMapping)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
            _dateTimePartName = dateTimePartName;
            _value = value;
            _fromFragment = _sqlExpressionFactory.Fragment($"{_dateTimePartName} FROM ");
            _timezoneFragment = _sqlExpressionFactory.Fragment(" AT TIME ZONE 'UTC'");
        }

        protected override void Print(ExpressionPrinter expressionPrinter)
        {
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            visitor.Visit(_fromFragment);
            var newValue = visitor.Visit(_value);
            visitor.Visit(_timezoneFragment);
            if (newValue != _value && newValue is SqlExpression newSqlValue)
            {
                return new SpannerTimestampExtractExpression(_sqlExpressionFactory, newSqlValue, _dateTimePartName);
            }
            return this;
        }

        public override bool Equals(object other)
        {
            if (other is SpannerTimestampExtractExpression o)
            {
                return _fromFragment.Equals(o._fromFragment) && _value.Equals(o._value) && _timezoneFragment.Equals(o._timezoneFragment);
            }
            return false;
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash *= 31 + _fromFragment.GetHashCode();
            hash *= 31 + _value.GetHashCode();
            hash *= _timezoneFragment.GetHashCode();
            return hash;
        }

        public override Expression Quote()
        {
#pragma warning disable EF9100 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
            return New(
                typeof(SpannerTimestampExtractExpression).GetConstructor(
                    [typeof(SqlExpression), typeof(SqlExpression), typeof(string), typeof(SpannerTimestampExtractExpression)])!,
                _value.Quote(),
                Constant(_dateTimePartName),
                RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));
#pragma warning restore EF9100 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
        }
    }
}
