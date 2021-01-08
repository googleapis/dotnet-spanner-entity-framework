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

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// Reads a forward-only stream of rows from a data source and keeps track of a running
    /// checksum for all data that have been seen.
    /// </summary>
    public sealed class SpannerDataReaderWithChecksum : DbDataReader, IRetriableStatement
    {
        private readonly SpannerCommand _spannerCommand;
        private int _numberOfReadCalls;
        private byte[] _currentChecksum = new byte[0];
        private SpannerException _firstException;

        internal SpannerDataReaderWithChecksum(
            SpannerRetriableTransaction transaction,
            SpannerDataReader spannerDataReader,
            SpannerCommand command)
        {
            Transaction = transaction;
            SpannerDataReader = spannerDataReader;
            _spannerCommand = (SpannerCommand)command.Clone();
        }

        internal SpannerRetriableTransaction Transaction { get; private set; }

        public override int Depth => SpannerDataReader.Depth;

        public override int FieldCount => SpannerDataReader.FieldCount;

        public override bool HasRows => SpannerDataReader.HasRows;

        public override bool IsClosed => SpannerDataReader.IsClosed;

        public override int RecordsAffected => SpannerDataReader.RecordsAffected;

        public override object this[string name] => SpannerDataReader[name];

        public override object this[int ordinal] => SpannerDataReader[ordinal];

        private SpannerDataReader SpannerDataReader;

        /// <inheritdoc />
        public override bool Read() => Task.Run(() => ReadAsync(CancellationToken.None)).ResultWithUnwrappedExceptions();

        /// <inheritdoc />
        public override Task<bool> ReadAsync(CancellationToken cancellationToken) =>
            Execute(async () =>
            {
                while (true)
                {
                    try
                    {
                        bool res = await SpannerDataReader.ReadAsync(cancellationToken).ConfigureAwait(false);
                        if (res)
                        {
                            _currentChecksum = CalculateNextChecksum(SpannerDataReader, _currentChecksum);
                        }
                        _numberOfReadCalls++;
                        return res;
                    }
                    catch (SpannerException e) when (e.ErrorCode == ErrorCode.Aborted)
                    {
                        // Retry the transaction and then retry the ReadAsync call.
                        await Transaction.RetryAsync(e, cancellationToken).ConfigureAwait(false);
                    }
                    catch (SpannerException e)
                    {
                        if (_firstException == null)
                        {
                            _firstException = e;
                        }
                        _numberOfReadCalls++;
                        throw e;
                    }
                }
            });

        internal static async Task<T> Execute<T>(Func<Task<T>> t)
        {
            var result = await t().ConfigureAwait(false);
            return result;
        }

        internal static byte[] CalculateNextChecksum(SpannerDataReader reader, byte[] currentChecksum)
        {
            int size = currentChecksum.Length;
            for (int i = 0; i < reader.FieldCount; i++)
            {
                size += reader.GetJsonValue(i).CalculateSize();
            }
            using var ms = new MemoryStream(size);
            ms.Write(currentChecksum, 0, currentChecksum.Length);
            using var cos = new CodedOutputStream(ms);
            for (int i = 0; i < reader.FieldCount; i++)
            {
                reader.GetJsonValue(i).WriteTo(cos);
            }
            // Flush the protobuf stream so everything is written to the memory stream.
            cos.Flush();
            // Then reset the memory stream to the start so the hash is calculated over all the bytes in the buffer.
            ms.Position = 0;
            SHA256 checksum = SHA256.Create();
            return checksum.ComputeHash(ms);
        }

        async Task IRetriableStatement.Retry(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds)
        {
            _spannerCommand.Transaction = transaction.SpannerTransaction;
            var reader = await _spannerCommand.ExecuteReaderAsync();
            int counter = 0;
            bool read = true;
            byte[] newChecksum = new byte[0];
            SpannerException newException = null;
            while (read && counter < _numberOfReadCalls)
            {
                try
                {
                    read = await reader.ReadAsync().ConfigureAwait(false);
                    if (read)
                    {
                        newChecksum = CalculateNextChecksum(reader, newChecksum);
                    }
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
                    SpannerDataReader = reader;
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
            return SpannerDataReader.GetBoolean(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return SpannerDataReader.GetByte(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return SpannerDataReader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override char GetChar(int ordinal)
        {
            return SpannerDataReader.GetChar(ordinal);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return SpannerDataReader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public override string GetDataTypeName(int ordinal)
        {
            return SpannerDataReader.GetDataTypeName(ordinal);
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return SpannerDataReader.GetDateTime(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return SpannerDataReader.GetDecimal(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return SpannerDataReader.GetDouble(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            return SpannerDataReader.GetEnumerator();
        }

        public override System.Type GetFieldType(int ordinal)
        {
            return SpannerDataReader.GetFieldType(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return SpannerDataReader.GetFloat(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return SpannerDataReader.GetGuid(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return SpannerDataReader.GetInt16(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return SpannerDataReader.GetInt32(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return SpannerDataReader.GetInt64(ordinal);
        }

        public override string GetName(int ordinal)
        {
            return SpannerDataReader.GetName(ordinal);
        }

        public override int GetOrdinal(string name)
        {
            return SpannerDataReader.GetOrdinal(name);
        }

        public override string GetString(int ordinal)
        {
            return SpannerDataReader.GetString(ordinal);
        }

        public override object GetValue(int ordinal)
        {
            return SpannerDataReader.GetValue(ordinal);
        }

        public override int GetValues(object[] values)
        {
            return SpannerDataReader.GetValues(values);
        }

        public override bool IsDBNull(int ordinal)
        {
            return SpannerDataReader.IsDBNull(ordinal);
        }

        public override bool NextResult()
        {
            return SpannerDataReader.NextResult();
        }
    }
}