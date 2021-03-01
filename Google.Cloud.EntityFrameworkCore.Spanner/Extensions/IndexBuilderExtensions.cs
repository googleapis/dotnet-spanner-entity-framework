﻿// Copyright 2021 Google LLC
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

using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Linq;
using System.Linq.Expressions;

namespace Microsoft.EntityFrameworkCore
{
    public static class IndexBuilderExtensions
    {
        public static IndexBuilder<TEntity> IsNullFiltered<TEntity>([NotNull] this IndexBuilder<TEntity> indexBuilder, bool isNullFiltered = true)
        {
            indexBuilder.Metadata.AddAnnotation(SpannerAnnotationNames.IsNullFilteredIndex, isNullFiltered);
            return indexBuilder;
        }

        public static IndexBuilder<TEntity> IsStoring<TEntity>(
            this IndexBuilder<TEntity> indexBuilder,
            Expression<Func<TEntity, object>> indexExpression)
            where TEntity : class
        {
            var storingColumns = indexExpression.GetPropertyAccessList();
            if (storingColumns.Count != 1)
            {
                throw new ArgumentException($"STORING clause is defined with a single key property, but {storingColumns.Count} values were passed to the 'IsStoring' method.");
            }

            indexBuilder.Metadata.AddAnnotation(SpannerAnnotationNames.IsStoringIndex, storingColumns.Select(c => c.Name).First());
            return indexBuilder;
        }
    }
}
