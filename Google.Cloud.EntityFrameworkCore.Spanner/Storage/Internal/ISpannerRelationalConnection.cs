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

using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    internal interface ISpannerRelationalConnection : IRelationalConnection
    {
        internal DdlExecutionStrategy DdlExecutionStrategy { get; set; }
        
        //Note: The RelationalConnection classes represent an EFCore level abstraction over the EFCore
        // providers.

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        ISpannerRelationalConnection CreateMasterConnection();
    }
}
