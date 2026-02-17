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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Microsoft.EntityFrameworkCore.Update;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal;

/// <summary>
/// This is internal functionality and not intended for public use.
/// 
/// Custom modification command for Cloud Spanner that adds support for EF Core 8's .ToJson()
/// owned entity serialization. Follows SQL Server's implementation pattern.
/// </summary>
internal class SpannerModificationCommand : ModificationCommand
{
    /// <summary>
    /// Creates a new SpannerModificationCommand instance for tracked modifications.
    /// </summary>
    public SpannerModificationCommand(in ModificationCommandParameters modificationCommandParameters)
        : base(modificationCommandParameters)
    {
    }

    /// <summary>
    /// Creates a new SpannerModificationCommand instance for non-tracked modifications (e.g., migrations).
    /// </summary>
    public SpannerModificationCommand(in NonTrackedModificationCommandParameters modificationCommandParameters)
        : base(modificationCommandParameters)
    {
    }

    /// <summary>
    /// Processes column modifications for single-property JSON updates.
    /// 
    /// For Cloud Spanner with .ToJson() support:
    /// - Disables partial JSON updates (not supported yet)
    /// - Uses full entity replacement strategy
    /// - JSON values are serialized to strings by EF Core's base implementation
    /// - Type mapping ensures proper JSON literal syntax for DML statements
    /// 
    /// TODO: Add support for partial JSON updates using JSON_SET/JSON_QUERY functions
    /// TODO: Add support for nested owned entities (currently only single-level OwnsOne supported)
    /// TODO: Add support for owned entity collections (OwnsMany)
    /// TODO: Optimize mutation path to handle JSON columns (currently DML-only)
    /// </summary>
    protected override void ProcessSinglePropertyJsonUpdate(ref ColumnModificationParameters parameters)
    {
        // Cloud Spanner doesn't support partial JSON updates yet.
        // Signal to EF Core that we need full entity serialization.
        // This prevents EF Core from trying to generate partial update statements.
        parameters = parameters with { JsonPath = null };
        
        base.ProcessSinglePropertyJsonUpdate(ref parameters);
    }
}
