// Copyright 2020 Google LLC
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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.Spanner.V1;
using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    public partial class AllColType
    {
        public int Id { get; set; }
        public short? ColShort { get; set; }
        public int? ColInt { get; set; }
        public long? ColLong { get; set; }
        public byte? ColByte { get; set; }
        public sbyte? ColSbyte { get; set; }
        public ulong? ColULong { get; set; }
        public ushort? ColUShort { get; set; }
        public SpannerNumeric? ColDecimal { get; set; }
        public uint? ColUint { get; set; }
        public bool? ColBool { get; set; }
        public SpannerDate? ColDate { get; set; }
        public DateTime? ColTimestamp { get; set; }
        public DateTime? ColCommitTimestamp { get; set; }
        public float? ColFloat { get; set; }
        public double? ColDouble { get; set; }
        public string ColString { get; set; }
        public Guid? ColGuid { get; set; }
        public byte[] ColBytes { get; set; }
        public SpannerNumeric[] ColDecimalArray { get; set; }
        public List<SpannerNumeric> ColDecimalList { get; set; }
        public string[] ColStringArray { get; set; }
        public List<string> ColStringList { get; set; }
        public bool[] ColBoolArray { get; set; }
        public List<bool> ColBoolList { get; set; }
        public double[] ColDoubleArray { get; set; }
        public List<double> ColDoubleList { get; set; }
        public long[] ColLongArray { get; set; }
        public List<long> ColLongList { get; set; }
        public SpannerDate[] ColDateArray { get; set; }
        public List<SpannerDate> ColDateList { get; set; }
        public DateTime[] ColTimestampArray { get; set; }
        public List<DateTime> ColTimestampList { get; set; }
        public byte[][] ColBytesArray { get; set; }
        public List<byte[]> ColBytesList { get; set; }
    }
}
