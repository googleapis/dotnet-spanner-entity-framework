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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Migrations
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class SpannerMigrationsAnnotationProviderTest
    {
        private readonly IServiceProvider _services;
        private readonly ModelBuilder _modelBuilder;
        private readonly SpannerRelationalAnnotationProvider _annotations;
        public SpannerMigrationsAnnotationProviderTest()
        {
            _services = SpannerTestHelpers.Instance.CreateContextServices();
            _modelBuilder = SpannerTestHelpers.Instance.CreateConventionBuilder(false, _services);
            _annotations = new SpannerRelationalAnnotationProvider(new RelationalAnnotationProviderDependencies());
        }

        [Fact]
        public void Resolves_is_null_filtered_index()
        {
            _modelBuilder
                .Entity<Entity>()
                .HasIndex(e => e.ColCommitTs)
                .IsNullFiltered();
            
            var model = _modelBuilder.Model.FinalizeModel();
            _services.GetRequiredService<IModelRuntimeInitializer>().Initialize(model);
            
            var entity = model.FindEntityType(typeof(Entity));
            var index = entity!.FindIndex(entity.FindProperty(nameof(Entity.ColCommitTs))!);
            var tableIndex = index!.GetMappedTableIndexes().First();
            var migrationAnnotations = _annotations.For(tableIndex, true).ToList();
            Assert.Contains(migrationAnnotations,
                a => a.Name == SpannerAnnotationNames.IsNullFilteredIndex && (bool)a.Value);
        }

        [Fact]
        public void Resolves_interleave_in_parent_entity()
        {
            _modelBuilder
                .Entity<Entity>()
                .InterleaveInParent(typeof(ParentEntity));
            
            var model = _modelBuilder.FinalizeModel();
            _services.GetRequiredService<IModelRuntimeInitializer>().Initialize(model);
            
            var entity = model.FindEntityType(typeof(Entity));
            var table = entity!.GetTableMappings().First().Table;
            var migrationAnnotations = _annotations.For(table, true).ToList();
            Assert.Contains(migrationAnnotations,
                a => a.Name == SpannerAnnotationNames.InterleaveInParent
                && a.Value.ToString() == "Google.Cloud.EntityFrameworkCore.Spanner.Tests.Migrations.SpannerMigrationsAnnotationProviderTest+ParentEntity");
        }

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
