using Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;

internal static class SpannerArrayTypes
{
    internal static readonly Cloud.Spanner.V1.Type SArrayOfDateType = new() { Code = TypeCode.Array, ArrayElementType = new Cloud.Spanner.V1.Type{Code = TypeCode.Date}};
    internal static readonly Cloud.Spanner.V1.Type SArrayOfJsonType = new() { Code = TypeCode.Array, ArrayElementType = new Cloud.Spanner.V1.Type{Code = TypeCode.Json}};

}