// Copyright 2021 Google Inc. All Rights Reserved.
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

using Google.Cloud.Spanner.Connection;
using NHibernate;
using NHibernate.AdoNet;
using NHibernate.Exceptions;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.NHibernate.Spanner
{
    public class SpannerBatcher : AbstractBatcher
    {
        private int _batchSize;
        private int _totalExpectedRowsAffected;
        private SpannerRetriableBatchCommand _currentBatch;
        private int _currentBatchStatementCount;
        
        public SpannerBatcher(ConnectionManager connectionManager, IInterceptor interceptor) : base(connectionManager, interceptor)
        {
            _batchSize = Factory.Settings.AdoBatchSize;
            _currentBatch = new SpannerRetriableBatchCommand();
        }

        public override int BatchSize
        {
            get => _batchSize;
            set => _batchSize = value;
        }

        protected override int CountOfStatementsInCurrentBatch => _currentBatchStatementCount;

        public override void AddToBatch(IExpectation expectation)
        {
            if (CountOfStatementsInCurrentBatch == 0)
            {
                CheckReaders();
            }

            _currentBatchStatementCount++;
            _totalExpectedRowsAffected += expectation.ExpectedRowCount;
            var batchUpdate = CurrentCommand as SpannerRetriableCommand;
            Prepare(batchUpdate);
            Driver.AdjustCommand(batchUpdate);
            _currentBatch.Add(batchUpdate!.Clone() as SpannerRetriableCommand);

            if (_currentBatchStatementCount >= _batchSize)
            {
                DoExecuteBatch(batchUpdate);
            }
        }

        protected override void DoExecuteBatch(DbCommand ps)
        {
            try
            {
                CheckReaders();
                int rowsAffected;
                
                try
                {
                    if (_currentBatchStatementCount == 1)
                    {
                        rowsAffected = ps.ExecuteNonQuery();
                    }
                    else
                    {
                        _currentBatch.Connection = ps.Connection as SpannerRetriableConnection;
                        _currentBatch.Transaction = ps.Transaction as SpannerRetriableTransaction;
                        // The maximum mutation count for a Spanner transaction is 20,000, so we don't
                        // have to worry that the total update count of a single batch will ever overflow
                        // an int.
                        rowsAffected = (int)_currentBatch.ExecuteNonQuery().Sum();
                    }
                }
                catch (DbException e)
                {
                    throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, e, "could not execute batch command.");
                }
                Expectations.VerifyOutcomeBatched(_totalExpectedRowsAffected, rowsAffected, ps);
            }
            finally
            {
                ClearCurrentBatch();
            }
        }

        private void ClearCurrentBatch()
        {
            _totalExpectedRowsAffected = 0;
            _currentBatchStatementCount = 0;
            _currentBatch = new SpannerRetriableBatchCommand();
        }

        protected override async Task DoExecuteBatchAsync(DbCommand ps, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await CheckReadersAsync(cancellationToken).ConfigureAwait(false);
                int rowsAffected;
                try
                {
                    if (_currentBatchStatementCount == 1)
                    {
                        rowsAffected = await ps.ExecuteNonQueryAsync(cancellationToken);
                    }
                    else
                    {
                        _currentBatch.Connection = ps.Connection as SpannerRetriableConnection;
                        _currentBatch.Transaction = ps.Transaction as SpannerRetriableTransaction;
                        // The maximum mutation count for a Spanner transaction is 20,000, so we don't
                        // have to worry that the total update count of a single batch will ever overflow
                        // an int.
                        rowsAffected =
                            (int)(await _currentBatch.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false))
                            .Sum();
                    }
                }
                catch (DbException e)
                {
                    throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, e, "could not execute batch command.");
                }
                Expectations.VerifyOutcomeBatched(_totalExpectedRowsAffected, rowsAffected, ps);
            }
            finally
            {
                ClearCurrentBatch();
            }
        }

        public override async Task AddToBatchAsync(IExpectation expectation, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (CountOfStatementsInCurrentBatch == 0)
            {
                await CheckReadersAsync(cancellationToken).ConfigureAwait(false);
            }

            _totalExpectedRowsAffected += expectation.ExpectedRowCount;
            var batchUpdate = CurrentCommand as SpannerRetriableCommand;
            await PrepareAsync(batchUpdate, cancellationToken).ConfigureAwait(false);
            Driver.AdjustCommand(batchUpdate);
            _currentBatch.Add(batchUpdate!.Clone() as SpannerRetriableCommand);
            _currentBatchStatementCount++;

            if (_currentBatchStatementCount >= _batchSize)
            {
                await DoExecuteBatchAsync(batchUpdate, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}