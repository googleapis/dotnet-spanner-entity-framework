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

using Google.Api.Gax;
using Google.Apis.Testing;
using Google.Cloud.Spanner.Data;
using Google.Protobuf;
using System;
using System.Collections;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// Reads a forward-only stream of rows from a data source and keeps track of a running
    /// checksum for all data that have been seen.
    /// </summary>
    internal sealed class SpannerDataReaderWithChecksum : DbDataReader, IRetriableStatement
    {
        private int _numberOfReadCalls;
        private readonly IncrementalHash _incrementalHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        private byte[] _finalChecksum;
        private bool _hashDisposed;
        private SpannerException _firstException;
        private readonly SpannerCommand _spannerCommand;

        internal SpannerDataReaderWithChecksum(
            SpannerRetriableTransaction transaction,
            SpannerDataReader spannerDataReader,
            SpannerCommand command)
        {
            Transaction = transaction;
            _spannerDataReader = spannerDataReader;
            _spannerCommand = (SpannerCommand)command.Clone();
        }

        internal SpannerRetriableTransaction Transaction { get; private set; }

        public override int Depth => _spannerDataReader.Depth;

        public override int FieldCount => _spannerDataReader.FieldCount;

        public override bool HasRows => _spannerDataReader.HasRows;

        public override bool IsClosed => _spannerDataReader.IsClosed;

        public override int RecordsAffected => _spannerDataReader.RecordsAffected;

        [VisibleForTestOnly]
        internal SpannerDataReader SpannerDataReader => _spannerDataReader;

        public override object this[string name] => _spannerDataReader[name];

        public override object this[int ordinal] => _spannerDataReader[ordinal];

        private SpannerDataReader _spannerDataReader;

        public override void Close()
        {
            _spannerDataReader.Close();
            base.Close();
        }

        /// <summary>
        /// Retrieves the current running checksum. If the reader has already been disposed,
        /// returns the final checksum snapshot taken at disposal.
        /// </summary>
        private byte[] GetChecksum()
        {
            return _finalChecksum ?? _incrementalHash.GetCurrentHash();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _spannerDataReader.Dispose();
                _spannerCommand.Dispose();
                if (!_hashDisposed)
                {
                    // Cache the final checksum before disposing the IncrementalHash, as the transaction
                    // retry engine may still need it for consistency checks if the transaction aborts
                    // after this reader has been closed/disposed by the client.
                    _finalChecksum = _incrementalHash.GetCurrentHash();
                    _incrementalHash.Dispose();
                    _hashDisposed = true;
                }
            }
            base.Dispose(disposing);
        }

        /// <inheritdoc />
        public override bool Read()
        {
            return ReadInternalAsync(async: false, CancellationToken.None).GetAwaiter().GetResult();
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            return ReadInternalAsync(async: true, cancellationToken).AsTask();
        }

        private async ValueTask<bool> ReadInternalAsync(bool async, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    bool res = async
                        ? await _spannerDataReader.ReadAsync(cancellationToken)
                        : _spannerDataReader.Read();
                    AppendRowToHash(_incrementalHash, _spannerDataReader, res);
                    _numberOfReadCalls++;
                    return res;
                }
                catch (SpannerException e) when (e.ErrorCode == ErrorCode.Aborted)
                {
                    // Retry the transaction and then retry the ReadAsync call.
                    if (async)
                    {
                        await Transaction.RetryAsync(e, cancellationToken);
                    }
                    else
                    {
                        Transaction.Retry(e);
                    }
                }
                catch (SpannerException e)
                {
                    if (_firstException == null)
                    {
                        _firstException = e;
                    }
                    _numberOfReadCalls++;
                    throw;
                }
            }
        }

        internal static async Task<T> Execute<T>(Func<Task<T>> t)
        {
            var result = await t().ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Serializes the row data and appends it to the IncrementalHash.
        /// </summary>
        private static void AppendRowToHash(IncrementalHash hash, SpannerDataReader reader, bool readResult)
        {
            // Use a fixed initial capacity (1KB) to avoid double traversal of fields via CalculateSize()
            // while minimizing buffer expansion allocations for typical row sizes.
            using var ms = new MemoryStream(1024);
            using (var cos = new CodedOutputStream(ms, 256))
            {
                Protobuf.WellKnownTypes.Value.ForBool(readResult).WriteTo(cos);
                if (readResult)
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        reader.GetJsonValue(i).WriteTo(cos);
                    }
                }
                // Flush the CodedOutputStream to ensure all bytes are flushed to the MemoryStream.
                cos.Flush();

                // Access the underlying buffer directly if possible to avoid allocating a new byte array via ToArray().
                // We must do this inside the using block before the CodedOutputStream is disposed, as disposing the
                // CodedOutputStream will close and dispose the underlying MemoryStream.
                if (ms.TryGetBuffer(out var buffer))
                {
                    hash.AppendData(buffer.Array, buffer.Offset, (int)ms.Length);
                }
                else
                {
                    hash.AppendData(ms.ToArray());
                }
            }
        }

        void IRetriableStatement.Retry(SpannerRetriableTransaction transaction, int timeoutSeconds)
        {
            RetryInternalAsync(transaction, async: false, CancellationToken.None).GetAwaiter().GetResult();
        }

        async Task IRetriableStatement.RetryAsync(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds)
        {
            await RetryInternalAsync(transaction, async: true, cancellationToken);
        }

        private async ValueTask<bool> RetryInternalAsync(SpannerRetriableTransaction transaction, bool async, CancellationToken cancellationToken)
        {
            _spannerCommand.Transaction = transaction.SpannerTransaction;
            var reader = async
                ? await _spannerCommand.ExecuteReaderAsync(cancellationToken)
                : (SpannerDataReader)_spannerCommand.ExecuteReader();
            bool keepReader = false;
            try
            {
                int counter = 0;
                bool read = true;
                using var retryHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                SpannerException newException = null;
                while (read && counter < _numberOfReadCalls)
                {
                    try
                    {
                        read = async
                            ? await reader.ReadAsync(cancellationToken)
                            : reader.Read();
                        AppendRowToHash(retryHash, reader, read);
                        counter++;
                    }
                    catch (SpannerException e) when (e.ErrorCode == ErrorCode.Aborted)
                    {
                        // Propagate Aborted errors to trigger a new retry.
                        throw;
                    }
                    catch (SpannerException e)
                    {
                        newException = e;
                        counter++;
                        break;
                    }
                }
                var originalChecksum = GetChecksum();
                var newChecksum = retryHash.GetHashAndReset();
                if (counter == _numberOfReadCalls
                    && newChecksum.SequenceEqual(originalChecksum)
                    && SpannerRetriableTransaction.SpannerExceptionsEqualForRetry(newException, _firstException))
                {
                    // Checksum is ok, we only need to replace the delegate result set if it's still open.
                    if (IsClosed)
                    {
                        reader.Close();
                    }
                    else
                    {
                        _spannerDataReader = reader;
                        keepReader = true;
                    }
                }
                else
                {
                    // The results are not equal, there is an actual concurrent modification, so we cannot
                    // continue the transaction.
                    throw new SpannerAbortedDueToConcurrentModificationException();
                }
            }
            finally
            {
                if (!keepReader)
                {
                    reader.Dispose();
                }
            }
            return keepReader;
        }

        public override bool GetBoolean(int ordinal)
        {
            return _spannerDataReader.GetBoolean(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return _spannerDataReader.GetByte(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return _spannerDataReader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override char GetChar(int ordinal)
        {
            return _spannerDataReader.GetChar(ordinal);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return _spannerDataReader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override string GetDataTypeName(int ordinal)
        {
            return _spannerDataReader.GetDataTypeName(ordinal);
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return _spannerDataReader.GetDateTime(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return _spannerDataReader.GetDecimal(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return _spannerDataReader.GetDouble(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            return _spannerDataReader.GetEnumerator();
        }

        public override System.Type GetFieldType(int ordinal)
        {
            return _spannerDataReader.GetFieldType(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return _spannerDataReader.GetFloat(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return _spannerDataReader.GetGuid(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return _spannerDataReader.GetInt16(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return _spannerDataReader.GetInt32(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return _spannerDataReader.GetInt64(ordinal);
        }

        public override string GetName(int ordinal)
        {
            return _spannerDataReader.GetName(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            return _spannerDataReader.GetOrdinal(name);
        }

        public override string GetString(int ordinal)
        {
            return _spannerDataReader.GetString(ordinal);
        }

        public override object GetValue(int ordinal)
        {
            return _spannerDataReader.GetValue(ordinal);
        }

        public override T GetFieldValue<T>(int ordinal)
        {
            return _spannerDataReader.GetFieldValue<T>(ordinal);
        }

        public override int GetValues(object[] values)
        {
            return _spannerDataReader.GetValues(values);
        }

        public override bool IsDBNull(int ordinal)
        {
            return _spannerDataReader.IsDBNull(ordinal);
        }

        public override bool NextResult()
        {
            return _spannerDataReader.NextResult();
        }
    }
}