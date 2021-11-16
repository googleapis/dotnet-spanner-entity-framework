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

using Google.Cloud.Spanner.Data;
using NHibernate.Engine;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Google.Cloud.NHibernate.Spanner
{
    /// <summary>
    /// A wrapper around a List<long?> that implements the NHibernate IUserType interface.
    /// Use this type for ARRAY<INT64> columns.
    /// </summary>
    public sealed class SpannerInt64Array : BaseSpannerArray<long?>
    {
        public SpannerInt64Array()
        {
        }

        public SpannerInt64Array(List<long?> array) : base(array)
        {
        }

        public override System.Type ReturnedType => typeof(SpannerInt64Array);

        public override object NullSafeGet(DbDataReader rs, string[] names, ISessionImplementor session, object owner) => 
            rs.IsDBNull(names[0]) ? null : new SpannerInt64Array(rs.GetFieldValue<List<long?>>(names[0]));

        public override object DeepCopy(object value)
        {
            if (value is SpannerInt64Array s)
            {
                return new SpannerInt64Array(s.Array == null ? null : new List<long?>(s.Array));
            }
            return new SpannerInt64Array();
        }

        protected override SpannerDbType GetArrayElementType() => SpannerDbType.Int64;
    }
}
