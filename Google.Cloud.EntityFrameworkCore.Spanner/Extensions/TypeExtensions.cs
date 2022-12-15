#nullable enable

using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions;

internal static class TypeExtensions
{
    internal static bool IsGenericList(this System.Type? type)
        => type is not null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>);

    internal static bool IsArrayOrGenericList(this System.Type type)
        => type.IsArray || type.IsGenericList();
}