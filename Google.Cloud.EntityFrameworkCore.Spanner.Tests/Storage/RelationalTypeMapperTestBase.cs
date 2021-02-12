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

using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Storage
{
    public abstract class RelationalTypeMapperTestBase
    {
        protected IMutableEntityType CreateEntityType()
            => CreateModel().FindEntityType(typeof(MyType));

        protected IMutableModel CreateModel()
        {
            var builder = CreateModelBuilder();

            builder.Entity<MyType>().Property(e => e.Id).HasColumnType("INT64");
            return builder.Model;
        }

        protected virtual ModelBuilder CreateModelBuilder() => SpannerTestHelpers.Instance.CreateConventionBuilder();

        protected class MyType
        {
            public long Id { get; set; }
        }
    }
}
