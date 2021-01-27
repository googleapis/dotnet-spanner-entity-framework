using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using System;
using System.Collections.Generic;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Metadata.Conventions.Infrastructure
{
    public class SpannerConventionSetBuilder : RelationalConventionSetBuilder
    {
        public SpannerConventionSetBuilder(
            [NotNull] ProviderConventionSetBuilderDependencies dependencies,
            [NotNull] RelationalConventionSetBuilderDependencies relationalDependencies) : base(dependencies, relationalDependencies)
        {
        }
    }
}
