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

namespace Google.Cloud.Spanner.Connection
{
    /// <summary>
    /// Reads a forward-only stream of rows from a data source and keeps track of a running
    /// checksum for all data that have been seen.
    /// </summary>
    internal sealed class SpannerDataReaderWithChecksum : DbDataReader, IRetriableStatement
    {
        private int _numberOfReadCalls;
        private byte[] _currentChecksum = new byte[0];
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

        public override object this[string name] => _spannerDataReader[name];

        public override object this[int ordinal] => _spannerDataReader[ordinal];

        private SpannerDataReader _spannerDataReader;

        /// <inheritdoc />
        public override bool Read() => Task.Run(() => ReadAsync(CancellationToken.None)).ResultWithUnwrappedExceptions();

        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    bool res = await _spannerDataReader.ReadAsync(cancellationToken);
                    _currentChecksum = CalculateNextChecksum(_spannerDataReader, _currentChecksum, res);
                    _numberOfReadCalls++;
                    return res;
                }
                catch (SpannerException e) when (e.ErrorCode == ErrorCode.Aborted)
                {
                    // Retry the transaction and then retry the ReadAsync call.
                    await Transaction.RetryAsync(e, cancellationToken);
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

        internal static byte[] CalculateNextChecksum(SpannerDataReader reader, byte[] currentChecksum, bool readResult)
        {
            int size = currentChecksum.Length;
            size += Protobuf.WellKnownTypes.Value.ForBool(readResult).CalculateSize();
            if (readResult)
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    size += reader.GetJsonValue(i).CalculateSize();
                }
            }
            using var ms = new MemoryStream(size);
            ms.Write(currentChecksum, 0, currentChecksum.Length);
            using var cos = new CodedOutputStream(ms);
            Protobuf.WellKnownTypes.Value.ForBool(readResult).WriteTo(cos);
            if (readResult)
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    reader.GetJsonValue(i).WriteTo(cos);
                }
            }
            // Flush the protobuf stream so everything is written to the memory stream.
            cos.Flush();
            // Then reset the memory stream to the start so the hash is calculated over all the bytes in the buffer.
            ms.Position = 0;
            SHA256 checksum = SHA256.Create();
            return checksum.ComputeHash(ms);
        }

        async Task IRetriableStatement.RetryAsync(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds)
        {
            _spannerCommand.Transaction = transaction.SpannerTransaction;
            var reader = await _spannerCommand.ExecuteReaderAsync(cancellationToken);
            int counter = 0;
            bool read = true;
            byte[] newChecksum = new byte[0];
            SpannerException newException = null;
            while (read && counter < _numberOfReadCalls)
            {
                try
                {
                    read = await reader.ReadAsync(cancellationToken);
                    newChecksum = CalculateNextChecksum(reader, newChecksum, read);
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
            if (counter == _numberOfReadCalls
                && newChecksum.SequenceEqual(_currentChecksum)
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
                }
            }
            else
            {
                // The results are not equal, there is an actual concurrent modification, so we cannot
                // continue the transaction.
                throw new SpannerAbortedDueToConcurrentModificationException();
            }
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