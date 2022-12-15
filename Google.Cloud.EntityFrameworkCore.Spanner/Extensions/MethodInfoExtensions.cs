using System.Reflection;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions;

internal static class MethodInfoExtensions
{
    internal static bool IsClosedFormOf(
        this MethodInfo methodInfo, MethodInfo genericMethod)
        => methodInfo.IsGenericMethod
           && Equals(
               methodInfo.GetGenericMethodDefinition(),
               genericMethod);
}