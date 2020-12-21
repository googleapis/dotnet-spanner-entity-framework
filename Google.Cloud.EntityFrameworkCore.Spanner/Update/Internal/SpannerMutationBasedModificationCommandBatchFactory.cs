// Copyright 2020, Google Inc. All rights reserved.
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

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerMutationBasedModificationCommandBatchFactory : IModificationCommandBatchFactory
    {
        private readonly IDiagnosticsLogger<DbLoggerCategory.Database.Command> _logger;
        private readonly IRelationalTypeMappingSource _typeMapper;

        /// <summary>
        /// </summary>
        /// <param name="typeMapper"></param>
        /// <param name="logger"></param>
        public SpannerMutationBasedModificationCommandBatchFactory(IRelationalTypeMappingSource typeMapper,
            IDiagnosticsLogger<DbLoggerCategory.Database.Command> logger)
        {
            _typeMapper = typeMapper;
            _logger = logger;
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public virtual ModificationCommandBatch Create()
            => new SpannerMutationBasedModificationCommandBatch(_typeMapper, _logger);
    }
}
