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

using System;
using System.Collections.Generic;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.Spanner.V1;
using System.Text.Json;
using SpannerDate = Google.Cloud.EntityFrameworkCore.Spanner.Storage.SpannerDate;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class TableWithAllColumnTypes : IDisposable
    {
        public long ColInt64 { get; set; }
        public double? ColFloat64 { get; set; }
        public SpannerNumeric? ColNumeric { get; set; }
        public bool? ColBool { get; set; }
        public string ColString { get; set; }
        public string ColStringMax { get; set; }
        public byte[] ColBytes { get; set; }
        public byte[] ColBytesMax { get; set; }
        public SpannerDate? ColDate { get; set; }
        public DateTime? ColTimestamp { get; set; }
        public JsonDocument ColJson { get; set; }
        public DateTime? ColCommitTs { get; set; }
        public List<Nullable<long>> ColInt64Array { get; set; }
        public List<Nullable<double>> ColFloat64Array { get; set; }
        public List<Nullable<SpannerNumeric>> ColNumericArray { get; set; }
        public List<Nullable<bool>> ColBoolArray { get; set; }
        public List<string> ColStringArray { get; set; }
        public List<string> ColStringMaxArray { get; set; }
        public List<byte[]> ColBytesArray { get; set; }
        public List<byte[]> ColBytesMaxArray { get; set; }
        public List<Nullable<SpannerDate>> ColDateArray { get; set; }
        public List<Nullable<DateTime>> ColTimestampArray { get; set; }
        public List<JsonDocument> ColJsonArray { get; set; }
        public string ColComputed { get; set; }
        public string ASC { get; set; }

        public void Dispose()
        {
            ColJson?.Dispose();
            if (ColJsonArray != null)
            {
                foreach (var json in ColJsonArray)
                {
                    json?.Dispose();
                }
            }
        }
    }
}
