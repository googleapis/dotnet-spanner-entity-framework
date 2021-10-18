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

using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Linq;
using System.Linq.Expressions;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions
{
    public static class IndexBuilderExtensions
    {
        /// <summary>
        /// Marks an index as NULL_FILTERED.
        /// </summary>
        /// <param name="indexBuilder">The builder for the index to modify</param>
        /// <param name="isNullFiltered">Specifies whether the index should filter null values or not</param>
        /// <returns> The same builder instance so that multiple configuration calls can be chained. </returns>
        public static IndexBuilder<TEntity> IsNullFiltered<TEntity>([NotNull] this IndexBuilder<TEntity> indexBuilder, bool isNullFiltered = true)
        {
            indexBuilder.Metadata.AddAnnotation(SpannerAnnotationNames.IsNullFilteredIndex, isNullFiltered);
            return indexBuilder;
        }

        /// <summary>
        /// Marks an index as STORING one or more additional columns.
        /// </summary>
        /// <param name="indexBuilder">The builder for the index to modify</param>
        /// <param name="storingPropertiesExpression">An expression that should return an array of properties that
        /// should be stored by the index.</param>
        /// <typeparam name="TEntity">The entity type that contains the index</typeparam>
        /// <returns> The same builder instance so that multiple configuration calls can be chained. </returns>
        public static IndexBuilder<TEntity> Storing<TEntity>(
            this IndexBuilder<TEntity> indexBuilder,
            Expression<Func<TEntity, object>> storingPropertiesExpression)
            where TEntity : class
        {
            indexBuilder.Metadata.AddAnnotation(SpannerAnnotationNames.Storing,
                storingPropertiesExpression.GetPropertyAccessList().Select(c => c.Name).ToArray());
            return indexBuilder;
        }
    }
}
