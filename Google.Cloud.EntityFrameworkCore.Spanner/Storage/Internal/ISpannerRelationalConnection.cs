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

using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.Spanner.Data;
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

        /// <summary>
        /// Begins a read/write transaction on this connection with the given transaction tag.
        /// </summary>
        IDbContextTransaction BeginTransaction(string tag);

        /// <summary>
        /// Begins a read/write transaction on this connection with the given isolation level and transaction tag.
        /// </summary>
        IDbContextTransaction BeginTransaction(IsolationLevel isolationLevel, string tag);

        /// <summary>
        /// Begins a read/write transaction on this connection with the given transaction tag.
        /// </summary>
        Task<IDbContextTransaction> BeginTransactionAsync(string tag, CancellationToken cancellationToken = default);

        /// <summary>
        /// Begins a read/write transaction on this connection with the given isolation level and transaction tag.
        /// </summary>
        Task<IDbContextTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, string tag, CancellationToken cancellationToken = default);

        /// <summary>
        /// Begins a read-only transaction on this connection.
        /// </summary>
        IDbContextTransaction BeginReadOnlyTransaction();

        /// <summary>
        /// Begins a read-only transaction with the specified <see cref="TimestampBound"/> on this connection.
        /// </summary>
        IDbContextTransaction BeginReadOnlyTransaction(TimestampBound timestampBound);

        /// <summary>
        /// Begins a read-only transaction on this connection.
        /// </summary>
        Task<IDbContextTransaction> BeginReadOnlyTransactionAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Begins a read-only transaction with the specified <see cref="TimestampBound"/> on this connection.
        /// </summary>
        Task<IDbContextTransaction> BeginReadOnlyTransactionAsync(TimestampBound timestampBound, CancellationToken cancellationToken = default);
    }
}
