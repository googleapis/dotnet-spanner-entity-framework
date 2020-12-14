using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Scaffolding.Internal
{
    public class SpannerCodeGenerator : ProviderCodeGenerator
    {
        public SpannerCodeGenerator(ProviderCodeGeneratorDependencies dependencies)
            : base(dependencies)
        {
        }

        public override MethodCallCodeFragment GenerateUseProvider(string connectionString, MethodCallCodeFragment providerOptions)
        => new MethodCallCodeFragment(
                nameof(SpannerDbContextOptionsExtensions.UseSpanner),
                providerOptions == null
                    ? new object[] { connectionString }
                    : new object[] { connectionString, new NestedClosureCodeFragment("x", providerOptions) });
    }
}
