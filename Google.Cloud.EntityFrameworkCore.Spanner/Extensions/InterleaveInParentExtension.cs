// Copyright 2021, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions
{
    public static class InterleaveInParentExtension
    {
        /// <summary>
        /// Indicates that a table should be interleaved in a parent table.
        /// </summary>
        /// <param name="builder">The entity builder to modify (the child entity)</param>
        /// <param name="parentEntity">The parent entity that the child entity should be interleaved in</param>
        /// <param name="onDelete">The optional action that should be executed for the child records when a parent
        /// record is deleted</param>
        /// <typeparam name="TEntity">The child entity type</typeparam>
        /// <returns> The same builder instance so that multiple configuration calls can be chained. </returns>
        public static EntityTypeBuilder<TEntity> InterleaveInParent<TEntity>(
            this EntityTypeBuilder<TEntity> builder, System.Type parentEntity,
            OnDelete onDelete = OnDelete.NoAction)
            where TEntity : class
        {
            builder.Metadata.AddAnnotation(SpannerAnnotationNames.InterleaveInParent, parentEntity.FullName);
            builder.Metadata.AddAnnotation(SpannerAnnotationNames.InterleaveInParentOnDelete, onDelete);
            return builder;
        }
    }
}
