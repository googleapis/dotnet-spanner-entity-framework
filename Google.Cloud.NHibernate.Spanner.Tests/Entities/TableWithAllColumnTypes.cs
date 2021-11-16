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
using System.Linq;
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

        public override bool Equals(object other)
        {
            if (other is TableWithAllColumnTypes o)
            {
                return Equals(ColInt64, o.ColInt64)
                       && Equals(ColFloat64, o.ColFloat64)
                       && Equals(ColNumeric, o.ColNumeric)
                       && Equals(ColBool, o.ColBool)
                       && Equals(ColString, o.ColString)
                       && Equals(ColStringMax, o.ColStringMax)
                       && Equals(ColBytes, o.ColBytes)
                       && Equals(ColBytesMax, o.ColBytesMax)
                       && Equals(ColDate, o.ColDate)
                       && Equals(ColTimestamp, o.ColTimestamp)
                       && Equals(ColJson, o.ColJson)
                       && Equals(ColCommitTs, o.ColCommitTs)
                       && Equals(ColComputed, o.ColComputed)
                       && Equals(ColInt64Array?.Array, o.ColInt64Array?.Array) || ColInt64Array != null && o.ColInt64Array != null && ColInt64Array.Array.SequenceEqual(o.ColInt64Array.Array)
                       && Equals(ColFloat64Array?.Array, o.ColFloat64Array?.Array) || ColFloat64Array != null && o.ColFloat64Array != null && ColFloat64Array.Array.SequenceEqual(o.ColFloat64Array.Array)
                       && Equals(ColNumericArray?.Array, o.ColNumericArray?.Array) || ColNumericArray != null && o.ColNumericArray != null && ColNumericArray.Array.SequenceEqual(o.ColNumericArray.Array)
                       && Equals(ColBoolArray?.Array, o.ColBoolArray?.Array) || ColBoolArray != null && o.ColBoolArray != null && ColBoolArray.Array.SequenceEqual(o.ColBoolArray.Array)
                       && Equals(ColStringArray?.Array, o.ColStringArray?.Array) || ColStringArray != null && o.ColStringArray != null && ColStringArray.Array.SequenceEqual(o.ColStringArray.Array)
                       && Equals(ColStringMaxArray?.Array, o.ColStringMaxArray?.Array) || ColStringMaxArray != null && o.ColStringMaxArray != null && ColStringMaxArray.Array.SequenceEqual(o.ColStringMaxArray.Array)
                       && Equals(ColBytesArray?.Array, o.ColBytesArray?.Array) || ColBytesArray != null && o.ColBytesArray != null && ColBytesArray.Array.SequenceEqual(o.ColBytesArray.Array)
                       && Equals(ColBytesMaxArray?.Array, o.ColBytesMaxArray?.Array) || ColBytesMaxArray != null && o.ColBytesMaxArray != null && ColBytesMaxArray.Array.SequenceEqual(o.ColBytesMaxArray.Array)
                       && Equals(ColDateArray?.Array, o.ColDateArray?.Array) || ColDateArray != null && o.ColDateArray != null && ColDateArray.Array.SequenceEqual(o.ColDateArray.Array)
                       && Equals(ColTimestampArray?.Array, o.ColTimestampArray?.Array) || ColTimestampArray != null && o.ColTimestampArray != null && ColTimestampArray.Array.SequenceEqual(o.ColTimestampArray.Array)
                       && Equals(ColJsonArray?.Array, o.ColJsonArray?.Array) || ColJsonArray != null && o.ColJsonArray != null && ColJsonArray.Array.SequenceEqual(o.ColJsonArray.Array)
                    ;
            }
            return false;
        }
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
