using Microsoft.EntityFrameworkCore.Infrastructure.Internal;

namespace Microsoft.EntityFrameworkCore.Infrastructure
{
    public class SpannerDbContextOptionsBuilder
           : RelationalDbContextOptionsBuilder<SpannerDbContextOptionsBuilder, SpannerOptionsExtension>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SpannerDbContextOptionsBuilder" /> class.
        /// </summary>
        /// <param name="optionsBuilder"> The options builder. </param>
        public SpannerDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
            : base(optionsBuilder)
        {
        }
    }
}
