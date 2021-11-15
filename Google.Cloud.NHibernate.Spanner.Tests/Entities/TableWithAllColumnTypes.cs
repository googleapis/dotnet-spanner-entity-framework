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

using Google.Cloud.Spanner.V1;
using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;
using System;
using System.Collections.Generic;
using System.Text.Json;

namespace Google.Cloud.NHibernate.Spanner.Tests.Entities
{
    public class TableWithAllColumnTypes
    {
        public long ColInt64 { get; set; }
        public double? ColFloat64 { get; set; }
        public SpannerNumeric ColNumeric { get; set; }
        public bool? ColBool { get; set; }
        public string ColString { get; set; }
        public string ColStringMax { get; set; }
        public byte[] ColBytes { get; set; }
        public byte[] ColBytesMax { get; set; }
        public SpannerDate ColDate { get; set; }
        public DateTime? ColTimestamp { get; set; }
        public SpannerJson ColJson { get; set; }
        public DateTime? ColCommitTs { get; set; }
        public SpannerInt64Array ColInt64Array { get; set; }
        public SpannerFloat64Array ColFloat64Array { get; set; }
        public SpannerNumericArray ColNumericArray { get; set; }
        public SpannerBoolArray ColBoolArray { get; set; }
        public SpannerStringArray ColStringArray { get; set; }
        public SpannerStringArray ColStringMaxArray { get; set; }
        public SpannerBytesArray ColBytesArray { get; set; }
        public SpannerBytesArray ColBytesMaxArray { get; set; }
        public SpannerDateArray ColDateArray { get; set; }
        public SpannerTimestampArray ColTimestampArray { get; set; }
        public SpannerJsonArray ColJsonArray { get; set; }
        public string ColComputed { get; set; }
        public string ASC { get; set; }
    }

    public class TableWithAllColumnTypesMapping : ClassMapping<TableWithAllColumnTypes>
    {
        public TableWithAllColumnTypesMapping()
        {
            Id(x => x.ColInt64);
            Property(x => x.ColFloat64);
            Property(x => x.ColNumeric);
            Property(x => x.ColBool);
            Property(x => x.ColString);
            Property(x => x.ColStringMax);
            Property(x => x.ColBytes);
            Property(x => x.ColBytesMax);
            Property(x => x.ColDate);
            Property(x => x.ColTimestamp);
            Property(x => x.ColJson);
            Property(x => x.ColCommitTs);
            Property(x => x.ColInt64Array);
            Property(x => x.ColFloat64Array);
            Property(x => x.ColNumericArray);
            Property(x => x.ColBoolArray);
            Property(x => x.ColStringArray);
            Property(x => x.ColStringMaxArray);
            Property(x => x.ColBytesArray);
            Property(x => x.ColBytesMaxArray);
            Property(x => x.ColDateArray);
            Property(x => x.ColTimestampArray);
            Property(x => x.ColJsonArray);
            Property(x => x.ColComputed, mapper => mapper.Generated(PropertyGeneration.Always));
            Property(x => x.ASC);
        }
    }
}
