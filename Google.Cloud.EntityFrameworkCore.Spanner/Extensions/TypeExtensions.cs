// Copyright 2022, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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