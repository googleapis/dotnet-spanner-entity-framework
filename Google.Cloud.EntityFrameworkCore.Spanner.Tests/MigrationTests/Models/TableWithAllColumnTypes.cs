// Copyright 2021 Google LLC
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

using System;
using System.Collections.Generic;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.Spanner.V1;
using SpannerDate = Google.Cloud.EntityFrameworkCore.Spanner.Storage.SpannerDate;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests.Models
{
    public partial class TableWithAllColumnTypes
    {
        public long ColSequence { get; set; }
        public long ColInt64 { get; set; }
        public double? ColFloat64 { get; set; }
        public SpannerNumeric? ColNumeric { get; set; }
        public bool? ColBool { get; set; }
        public string ColString { get; set; }
        public string ColStringMax { get; set; }
        public char? ColChar { get; set; }
        public byte[] ColBytes { get; set; }
        public byte[] ColBytesMax { get; set; }
        public SpannerDate? ColDate { get; set; }
        public DateTime? ColTimestamp { get; set; }
        public DateTime? ColCommitTs { get; set; }
        public List<long?> ColInt64Array { get; set; }
        public List<double?> ColFloat64Array { get; set; }
        public List<SpannerNumeric?> ColNumericArray { get; set; }
        public List<bool?> ColBoolArray { get; set; }
        public List<string> ColStringArray { get; set; }
        public List<string> ColStringMaxArray { get; set; }
        public List<byte[]> ColBytesArray { get; set; }
        public List<byte[]> ColBytesMaxArray { get; set; }
        public List<SpannerDate?> ColDateArray { get; set; }
        public List<DateTime?> ColTimestampArray { get; set; }
        public Guid? ColGuid { get; set; }
        public string ColComputed { get; set; }
    }
}
