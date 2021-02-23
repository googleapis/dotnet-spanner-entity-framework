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

using Google.Cloud.EntityFrameworkCore.Spanner.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using System.Linq;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Scaffolding
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class SpannerCodeGeneratorTest
    {
        [Fact]
        public virtual void Use_provider_method_is_generated_correctly()
        {
            var codeGenerator = new SpannerCodeGenerator(
                new ProviderCodeGeneratorDependencies(Enumerable.Empty<IProviderCodeGeneratorPlugin>()));

            var result = codeGenerator.GenerateUseProvider("Source=projects/p1/instances/i1/databases/d1", providerOptions: null);
            Assert.Equal("UseSpanner", result.Method);
            Assert.Collection(
                result.Arguments,
                a => Assert.Equal("Source=projects/p1/instances/i1/databases/d1", a));
            Assert.Null(result.ChainedCall);
        }

        [Fact]
        public virtual void Use_provider_method_is_generated_correctly_with_options()
        {
            var codeGenerator = new SpannerCodeGenerator(
                new ProviderCodeGeneratorDependencies(Enumerable.Empty<IProviderCodeGeneratorPlugin>()));

            var providerOptions = new MethodCallCodeFragment("SetProviderOption");

            var result = codeGenerator.GenerateUseProvider("Source=projects/p1/instances/i1/databases/d1", providerOptions);

            Assert.Equal("UseSpanner", result.Method);
            Assert.Collection(
                result.Arguments,
                a => Assert.Equal("Source=projects/p1/instances/i1/databases/d1", a),
                a =>
                {
                    var nestedClosure = Assert.IsType<NestedClosureCodeFragment>(a);

                    Assert.Equal("x", nestedClosure.Parameter);
                    Assert.Same(providerOptions, nestedClosure.MethodCall);
                });
            Assert.Null(result.ChainedCall);
        }
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
