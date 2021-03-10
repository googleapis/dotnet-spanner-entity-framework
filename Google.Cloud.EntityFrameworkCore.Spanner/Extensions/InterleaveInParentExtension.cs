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

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;

namespace Microsoft.EntityFrameworkCore
{
    public static class InterleaveInParentExtension
    {
        public static EntityTypeBuilder<TEntity> InterleaveInParent<TEntity>(
            this EntityTypeBuilder<TEntity> builder, Type parentEntity,
            OnDelete onDelete = OnDelete.NoAction)
            where TEntity : class
        {
            builder.Metadata.AddAnnotation(SpannerAnnotationNames.InterleaveInParent, parentEntity.FullName);
            builder.Metadata.AddAnnotation(SpannerAnnotationNames.InterleaveInParentOnDelete, onDelete);
            return builder;
        }
    }
}
