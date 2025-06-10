// Copyright 2022, Google Inc. All rights reserved.
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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerRelationalAnnotationProvider : RelationalAnnotationProvider
    {
        /// <summary>
        /// Initializes a new instance of this class.
        /// </summary>
        /// <param name="dependencies"> Parameter object containing dependencies for this service. </param>
        public SpannerRelationalAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
            : base(dependencies)
        {
        }

        /// <inheritdoc />
        public override IEnumerable<IAnnotation> For(IColumn column, bool designTime)
        {
            var property = column.PropertyMappings.First().Property;
            var primaryKey = property.DeclaringType.ContainingEntityType.FindPrimaryKey();
            if (primaryKey is { Properties.Count: 1 }
                && primaryKey.Properties[0] == property
                && property.ValueGenerated == ValueGenerated.OnAdd
                && IsInteger(property.ClrType)
                && !HasConverter(property))
            {
                var defaultIdentityOptions = column.Table.Model.Model.GetIdentityOptions();
                yield return new Annotation(SpannerAnnotationNames.Identity, (defaultIdentityOptions ?? SpannerIdentityOptionsData.Default).Serialize());
            }

            foreach (var mapping in column.PropertyMappings)
            {
                var commitTimestampAnnotation = mapping.Property.FindAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp);
                if (commitTimestampAnnotation != null)
                {
                    yield return commitTimestampAnnotation;
                }
            }
        }
        
        private static bool HasConverter(IProperty property) => property.FindTypeMapping()?.Converter != null;
        private  static System.Type UnwrapNullableType(System.Type type) => Nullable.GetUnderlyingType(type) ?? type;
        private static bool IsInteger(System.Type type)
        {
            type = UnwrapNullableType(type);
            return type == typeof(int)
                   || type == typeof(long)
                   || type == typeof(short)
                   || type == typeof(byte)
                   || type == typeof(uint)
                   || type == typeof(ulong)
                   || type == typeof(ushort)
                   || type == typeof(sbyte)
                   || type == typeof(char);
        }
        
        /// <inheritdoc />
        public override IEnumerable<IAnnotation> For(ITable table, bool designTime)
        {
            var baseAnnotations = base.For(table, designTime);
            
            foreach (var mapping in table.EntityTypeMappings)
            {
                var interleaveInParentAnnotation = mapping.TypeBase.FindAnnotation(SpannerAnnotationNames.InterleaveInParent);
                if (interleaveInParentAnnotation != null)
                {
                    return baseAnnotations.Concat(new[]
                        {
                            interleaveInParentAnnotation,
                            mapping.TypeBase.FindAnnotation(SpannerAnnotationNames.InterleaveInParentOnDelete)
                        });
                }
            }
            return baseAnnotations;
        }

        /// <inheritdoc />
        public override IEnumerable<IAnnotation> For(ITableIndex tableIndex, bool designTime)
        {
            var indexAnnotations = base.For(tableIndex, designTime);

            foreach (var mapping in tableIndex.MappedIndexes)
            {
                var nullFilteredIndexAnnotation = mapping.FindAnnotation(SpannerAnnotationNames.IsNullFilteredIndex);
                if (nullFilteredIndexAnnotation != null)
                {
                    indexAnnotations = indexAnnotations.Concat(new[] { nullFilteredIndexAnnotation });
                }

                var storingIndexAnnotation = mapping.FindAnnotation(SpannerAnnotationNames.Storing);
                if (storingIndexAnnotation != null)
                {
                    indexAnnotations = indexAnnotations.Concat(new[] { storingIndexAnnotation });
                }
            }

            return indexAnnotations;
        }
    }
}
