// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Data.Common;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;

/// <inheritdoc />
public class SpannerRelationalTransactionFactory : RelationalTransactionFactory
{
    /// <summary>
    /// Only for internal use.
    /// </summary>
    public SpannerRelationalTransactionFactory(RelationalTransactionFactoryDependencies dependencies) :
        base(dependencies)
    {
    }

    /// <inheritdoc/>
    public override RelationalTransaction Create(
        IRelationalConnection connection,
        DbTransaction transaction,
        Guid transactionId,
        IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger,
        bool transactionOwned)
        => new SpannerRelationalTransaction(connection, transaction, transactionId, logger, transactionOwned, Dependencies.SqlGenerationHelper);
    
}