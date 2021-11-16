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
using System.Linq;
using System.Runtime.Intrinsics;

namespace Google.Cloud.NHibernate.Spanner
{
    /// <summary>
    /// A wrapper around a List<SpannerNumeric> that implements the NHibernate IUserType interface.
    /// Use this type for ARRAY<NUMERIC> columns.
    /// </summary>
    public sealed class SpannerNumericArray : BaseSpannerArray<Cloud.Spanner.V1.SpannerNumeric?>
    {
        public SpannerNumericArray()
        {
        }

        public SpannerNumericArray(List<Cloud.Spanner.V1.SpannerNumeric?> array) : base(array)
        {
        }

        public override System.Type ReturnedType => typeof(SpannerNumericArray);

        public override object NullSafeGet(DbDataReader rs, string[] names, ISessionImplementor session, object owner) => 
            rs.IsDBNull(names[0]) ? null : new SpannerNumericArray(
                rs.GetFieldValue<List<Cloud.Spanner.V1.SpannerNumeric?>>(names[0]));

        public override object DeepCopy(object value)
        {
            if (value is SpannerNumericArray s)
            {
                return new SpannerNumericArray(s.Array == null ? null : new List<Cloud.Spanner.V1.SpannerNumeric?>(s.Array));
            }
            return new SpannerNumericArray();
        }

        protected override SpannerDbType GetArrayElementType() => SpannerDbType.Numeric;
    }
}
