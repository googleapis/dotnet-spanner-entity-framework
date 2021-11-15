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
    /// A wrapper around a List<double?> that implements the NHibernate IUserType interface.
    /// Use this type for ARRAY<FLOAT64> columns.
    /// </summary>
    public sealed class SpannerFloat64Array : BaseSpannerArray<double?>
    {
        public SpannerFloat64Array()
        {
        }

        public SpannerFloat64Array(List<double?> array) : base(array)
        {
        }

        public override System.Type ReturnedType => typeof(SpannerFloat64Array);

        public override object NullSafeGet(DbDataReader rs, string[] names, ISessionImplementor session, object owner) => 
            rs.IsDBNull(names[0]) ? null : new SpannerFloat64Array(rs.GetFieldValue<List<double?>>(names[0]));

        public override object DeepCopy(object value) =>
            new SpannerFloat64Array(new List<double?>(Array));

        protected override SpannerDbType GetArrayElementType() => SpannerDbType.Float64;
    }
}
