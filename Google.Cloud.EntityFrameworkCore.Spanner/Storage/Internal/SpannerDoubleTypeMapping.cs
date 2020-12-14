using Google.Cloud.Spanner.Data;
using System;

namespace Microsoft.EntityFrameworkCore.Storage.Internal
{
    internal class SpannerDoubleTypeMapping : DoubleTypeMapping
    {
        public SpannerDoubleTypeMapping()
            : base(SpannerDbType.Float64.ToString(), null)
        {
        }

        public override RelationalTypeMapping Clone(string storeType, int? size)
            => new SpannerDoubleTypeMapping();

        protected override string GenerateNonNullSqlLiteral(object value)
            => base.GenerateNonNullSqlLiteral(Convert.ToDouble(value));
    }
}
