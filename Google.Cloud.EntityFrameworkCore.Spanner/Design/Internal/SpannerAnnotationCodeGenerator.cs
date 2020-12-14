using Microsoft.EntityFrameworkCore.Design;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Design.Internal
{
    public class SpannerAnnotationCodeGenerator : AnnotationCodeGenerator
    {
        public SpannerAnnotationCodeGenerator(AnnotationCodeGeneratorDependencies dependencies)
            : base(dependencies)
        {
        }
    }
}
