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

using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure
{

    /// <summary>
    /// This enum is used to determine when the Cloud Spanner Entity Framework Core provider should
    /// use Mutations and when it should use DML for inserts/updates/deletes. DML statements allow
    /// transactions to read their own writes, but DML statements are slow in comparison
    /// mutations, especially for large batches of small updates. Mutations do not allow
    /// read-your-writes semantics, as mutations are buffered in the client until Commit is called,
    /// but mutations execute significantly faster on the backend.
    /// 
    /// The Cloud Spanner Entity Framework Core provider therefore defaults to using mutations for
    /// implicit transactions, that is: when the application does not manually start a transaction
    /// on the database context. With implicit transactions the EF Core provider automatically starts
    /// a transaction when SaveChanges or SaveChangesAsync is called and commits this transaction if
    /// all operations succeeded.
    /// When the application manually starts a transaction, all inserts, updates and deletes will be
    /// executed as DML statements on the transaction. This allows the application to read the writes
    /// that have already been executed on the transaction.
    /// 
    /// An application can configure a DbContext to use either DML or Mutations for all updates by
    /// calling DbContextOptionsBuilder.UseMutations(MutationUsage).
    /// </summary>
    public enum MutationUsage
    {
        // Never use mutations, always use DML. This configuration is not recommended for most applications.
        Never,
        // Use mutations for implicit transactions and DML for manual transactions.
        // Mutations will not be used if the implicit transaction contains inserts with auto-generated
        // primary key values or any other values that need to be returned as part of the insert/update
        // statement.
        //
        // This is the default and is the appropriate configuration for most applications.
        ImplicitTransactions,
        // Always use mutations, never use DML. This will disable read-your-writes for manual transactions.
        // Use this for contexts that execute a large number of updates in manual transactions, if these
        // transactions do not need to read their own writes.
        Always
    }

    public class SpannerDbContextOptionsBuilder
           : RelationalDbContextOptionsBuilder<SpannerDbContextOptionsBuilder, SpannerOptionsExtension>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SpannerDbContextOptionsBuilder" /> class.
        /// </summary>
        /// <param name="optionsBuilder"> The options builder. </param>
        public SpannerDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
            : base(optionsBuilder)
        {
        }
    }
}
