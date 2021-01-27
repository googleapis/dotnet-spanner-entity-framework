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

using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using System.Linq;

namespace Microsoft.EntityFrameworkCore.Metadata.Conventions
{
    public class SpannerConventionSetBuilder : RelationalConventionSetBuilder
    {
        public SpannerConventionSetBuilder(
            [NotNull] ProviderConventionSetBuilderDependencies dependencies,
            [NotNull] RelationalConventionSetBuilderDependencies relationalDependencies)
            : base(dependencies, relationalDependencies)
        {
        }

        public override ConventionSet CreateConventionSet()
        {
            var conventionSet = base.CreateConventionSet();

            // Note: unit https://github.com/dotnet/efcore/issues/214 not fix need to remove ForeignKeyIndexConvention
            // from each of following Conventions.
            var foreignKeyAddedConvention = conventionSet.ForeignKeyAddedConventions.FirstOrDefault(f => f is ForeignKeyIndexConvention);
            if (foreignKeyAddedConvention != null)
            {
                conventionSet.ForeignKeyAddedConventions.Remove(foreignKeyAddedConvention);
            }

            var foreignKeyRemovedConvention = conventionSet.ForeignKeyRemovedConventions.FirstOrDefault(f => f is ForeignKeyIndexConvention);
            if (foreignKeyRemovedConvention != null)
            {
                conventionSet.ForeignKeyRemovedConventions.Remove(foreignKeyRemovedConvention);
            }

            var entityTypeBaseTypeChangedConvention = conventionSet.EntityTypeBaseTypeChangedConventions.FirstOrDefault(f => f is ForeignKeyIndexConvention);
            if (entityTypeBaseTypeChangedConvention != null)
            {
                conventionSet.EntityTypeBaseTypeChangedConventions.Remove(entityTypeBaseTypeChangedConvention);
            }

            var keyAddedConvention = conventionSet.KeyAddedConventions.FirstOrDefault(f => f is ForeignKeyIndexConvention);
            if (foreignKeyAddedConvention != null)
            {
                conventionSet.KeyAddedConventions.Remove(keyAddedConvention);
            }

            var keyRemovedConvention = conventionSet.KeyRemovedConventions.FirstOrDefault(f => f is ForeignKeyIndexConvention);
            if (foreignKeyAddedConvention != null)
            {
                conventionSet.KeyRemovedConventions.Remove(keyRemovedConvention);
            }

            var foreignKeyPropertiesChangedConvention = conventionSet.ForeignKeyPropertiesChangedConventions.FirstOrDefault(f => f is ForeignKeyIndexConvention);
            if (foreignKeyAddedConvention != null)
            {
                conventionSet.ForeignKeyPropertiesChangedConventions.Remove(foreignKeyPropertiesChangedConvention);
            }

            var foreignKeyUniquenessChangedConvention = conventionSet.ForeignKeyUniquenessChangedConventions.FirstOrDefault(f => f is ForeignKeyIndexConvention);
            if (foreignKeyAddedConvention != null)
            {
                conventionSet.ForeignKeyUniquenessChangedConventions.Remove(foreignKeyUniquenessChangedConvention);
            }

            var indexAddedConvention = conventionSet.IndexAddedConventions.FirstOrDefault(f => f is ForeignKeyIndexConvention);
            if (foreignKeyAddedConvention != null)
            {
                conventionSet.IndexAddedConventions.Remove(indexAddedConvention);
            }

            var indexRemovedConvention = conventionSet.IndexRemovedConventions.FirstOrDefault(f => f is ForeignKeyIndexConvention);
            if (foreignKeyAddedConvention != null)
            {
                conventionSet.IndexRemovedConventions.Remove(indexRemovedConvention);
            }

            var indexUniquenessChangedConvention = conventionSet.IndexUniquenessChangedConventions.FirstOrDefault(f => f is ForeignKeyIndexConvention);
            if (indexUniquenessChangedConvention != null)
            {
                conventionSet.IndexUniquenessChangedConventions.Remove(indexUniquenessChangedConvention);
            }

            return conventionSet;
        }
    }
}
