﻿// Copyright 2020, Google Inc. All rights reserved.
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

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal
{
    public class SpannerMigrationsAnnotationProvider : MigrationsAnnotationProvider
    {
        /// <summary>
        /// Initializes a new instance of this class.
        /// </summary>
        /// <param name="dependencies"> Parameter object containing dependencies for this service. </param>
        public SpannerMigrationsAnnotationProvider(MigrationsAnnotationProviderDependencies dependencies)
            : base(dependencies)
        {
        }

        public override IEnumerable<IAnnotation> For(IProperty property)
        {
            var baseAnnotations = base.For(property);

            // Commit Timestamp
            var commitTimestampAnnotation = property.FindAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp);
            if (commitTimestampAnnotation != null)
            {
                baseAnnotations = baseAnnotations.Concat(new[] { commitTimestampAnnotation });
            }

            return baseAnnotations;
        }

        public override IEnumerable<IAnnotation> For(IEntityType entityType)
        {
            var baseAnnotations = base.For(entityType);
            var interleaveInParentAttribute = GetAttribute<InterleaveInParentAttribute>(entityType.ClrType);
            return interleaveInParentAttribute == null ? baseAnnotations
              : baseAnnotations.Concat(new[] {
                  new Annotation(SpannerAnnotationNames.InterleaveInParent, interleaveInParentAttribute.ParentEntity.FullName),
                  new Annotation(SpannerAnnotationNames.InterleaveInParentOnDelete, interleaveInParentAttribute.OnDelete)
              });
        }

        public override IEnumerable<IAnnotation> For(IIndex index)
        {
            var indexAnnotations = base.For(index);
            var nullFilteredIndexAnnotation = index.FindAnnotation(SpannerAnnotationNames.IsNullFilteredIndex);
            if (nullFilteredIndexAnnotation != null)
            {
                indexAnnotations = indexAnnotations.Concat(new[] { nullFilteredIndexAnnotation });
            }

            var storingIndexAnnotation = index.FindAnnotation(SpannerAnnotationNames.IsStoringIndex);
            if (storingIndexAnnotation != null)
            {
                indexAnnotations = indexAnnotations.Concat(new[] { storingIndexAnnotation });
            }

            return indexAnnotations;
        }

        private static TAttribute GetAttribute<TAttribute>(MemberInfo memberInfo)
            where TAttribute : Attribute
        {
            if (memberInfo == null
                || !Attribute.IsDefined(memberInfo, typeof(TAttribute), inherit: true))
            {
                return null;
            }

            return memberInfo.GetCustomAttribute<TAttribute>(inherit: true);
        }
    }
}
