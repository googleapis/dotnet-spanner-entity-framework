// Copyright 2025, Google LLC
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

using Microsoft.EntityFrameworkCore.Update;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal;

/// <summary>
/// This is internal functionality and not intended for public use.
/// 
/// Factory for creating SpannerModificationCommand instances that support EF Core 8's .ToJson()
/// owned entity serialization.
/// </summary>
internal class SpannerModificationCommandFactory : IModificationCommandFactory
{
    /// <summary>
    /// Creates a tracked modification command.
    /// </summary>
    public virtual IModificationCommand CreateModificationCommand(
        in ModificationCommandParameters modificationCommandParameters)
        => new SpannerModificationCommand(modificationCommandParameters);

    /// <summary>
    /// Creates a non-tracked modification command (used for migrations).
    /// </summary>
    public virtual INonTrackedModificationCommand CreateNonTrackedModificationCommand(
        in NonTrackedModificationCommandParameters modificationCommandParameters)
        => new SpannerModificationCommand(modificationCommandParameters);
}
