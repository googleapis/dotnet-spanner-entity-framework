using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;

internal class SpannerDateDateTimeTypeMapping : SpannerDateTypeMapping
{
    public SpannerDateDateTimeTypeMapping() : base(typeof(DateTime))
    {
    }
}