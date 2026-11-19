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

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Query
{
    using Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;
    using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
    using Microsoft.EntityFrameworkCore.Query;
    using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
    using Microsoft.Extensions.DependencyInjection;
    using System;
    using Xunit;

    public class SpannerStringMethodTranslatorTest
    {
        private readonly ISqlExpressionFactory _sqlExpressionFactory;
        private readonly SpannerStringMethodTranslator _translator;

        public SpannerStringMethodTranslatorTest()
        {
            var serviceProvider = SpannerTestHelpers.Instance.CreateContextServices();
            _sqlExpressionFactory = serviceProvider.GetRequiredService<ISqlExpressionFactory>();
            _translator = new SpannerStringMethodTranslator(_sqlExpressionFactory);
        }

        [Fact]
        public void Translate_StringFormat_ReturnsNull_ForClientEvaluation()
        {
            var singleArgMethod = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object) });
            var twoArgMethod = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object), typeof(object) });
            var threeArgMethod = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object), typeof(object), typeof(object) });
            var varArgMethod = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object[]) });

            Assert.NotNull(singleArgMethod);
            Assert.NotNull(twoArgMethod);
            Assert.NotNull(threeArgMethod);
            Assert.NotNull(varArgMethod);

            var formatExpression = _sqlExpressionFactory.Constant("{0}: {1}");
            var firstArgument = _sqlExpressionFactory.Constant("Alice");
            var secondArgument = _sqlExpressionFactory.Constant("Morrison");

            Assert.Null(_translator.Translate(null, singleArgMethod, new[] { formatExpression, firstArgument }, null));
            Assert.Null(_translator.Translate(null, twoArgMethod, new[] { formatExpression, firstArgument, secondArgument }, null));
            Assert.Null(_translator.Translate(null, threeArgMethod, new[] { formatExpression, firstArgument, secondArgument, firstArgument }, null));
            Assert.Null(_translator.Translate(null, varArgMethod, new[] { formatExpression, firstArgument }, null));
        }
    }
}
