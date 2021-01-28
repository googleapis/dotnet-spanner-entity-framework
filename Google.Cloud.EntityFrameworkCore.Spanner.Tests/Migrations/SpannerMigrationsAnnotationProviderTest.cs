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

using Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Migrations
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class SpannerMigrationsAnnotationProviderTest
    {
        private readonly ModelBuilder _modelBuilder;
        private readonly SpannerMigrationsAnnotationProvider _annotations;
        public SpannerMigrationsAnnotationProviderTest()
        {
            _modelBuilder = SpannerTestHelpers.Instance.CreateConventionBuilder( /*skipValidation: true*/);
            _annotations = new SpannerMigrationsAnnotationProvider(new MigrationsAnnotationProviderDependencies());
        }

        [Fact]
        public void Resolves_is_null_filtered_index()
        {
            var property = _modelBuilder.Entity<Entity>().HasIndex(e => e.ColCommitTs)
                   .IsNullFiltered().Metadata;
            var migrationAnnotations = _annotations.For(property).ToList();
            Assert.Contains(migrationAnnotations,
                a => a.Name == SpannerAnnotationNames.IsNullFilteredIndex && (bool)a.Value);
        }

        [Fact]
        public void Resolves_interleave_in_parent_entity()
        {
            var entity = _modelBuilder.Entity<Entity>().Metadata;
            var migrationAnnotations = _annotations.For(entity).ToList();
            Assert.Contains(migrationAnnotations,
                a => a.Name == SpannerAnnotationNames.InterleaveInParent
                && a.Value.ToString() == "Google.Cloud.EntityFrameworkCore.Spanner.Tests.Migrations.SpannerMigrationsAnnotationProviderTest+ParentEntity");
        }


        [InterleaveInParent(typeof(ParentEntity))]
        private class Entity
        {
            public int EntityId { get; set; }
            public string IndexedProp { get; set; }
            public string IncludedProp { get; set; }
            public DateTime? ColCommitTs { get; set; }
        }

        private class ParentEntity
        {
            public int Id { get; set; }
            public int EntityId { get; set; }
        }
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
