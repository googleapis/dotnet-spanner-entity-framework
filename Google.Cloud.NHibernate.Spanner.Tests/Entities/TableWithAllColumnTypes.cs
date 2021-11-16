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
        public virtual long ColInt64 { get; set; }
        public virtual double? ColFloat64 { get; set; }
        public virtual SpannerNumeric ColNumeric { get; set; }
        public virtual bool? ColBool { get; set; }
        public virtual string ColString { get; set; }
        public virtual string ColStringMax { get; set; }
        public virtual byte[] ColBytes { get; set; }
        public virtual byte[] ColBytesMax { get; set; }
        public virtual SpannerDate ColDate { get; set; }
        public virtual DateTime? ColTimestamp { get; set; }
        public virtual SpannerJson ColJson { get; set; }
        public virtual DateTime? ColCommitTs { get; set; }
        public virtual SpannerInt64Array ColInt64Array { get; set; }
        public virtual SpannerFloat64Array ColFloat64Array { get; set; }
        public virtual SpannerNumericArray ColNumericArray { get; set; }
        public virtual SpannerBoolArray ColBoolArray { get; set; }
        public virtual SpannerStringArray ColStringArray { get; set; }
        public virtual SpannerStringArray ColStringMaxArray { get; set; }
        public virtual SpannerBytesArray ColBytesArray { get; set; }
        public virtual SpannerBytesArray ColBytesMaxArray { get; set; }
        public virtual SpannerDateArray ColDateArray { get; set; }
        public virtual SpannerTimestampArray ColTimestampArray { get; set; }
        public virtual SpannerJsonArray ColJsonArray { get; set; }
        public virtual string ColComputed { get; set; }
        public virtual string ASC { get; set; }
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
