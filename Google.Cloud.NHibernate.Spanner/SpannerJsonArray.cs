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

namespace Google.Cloud.NHibernate.Spanner
{
    /// <summary>
    /// A wrapper around a List<string> that implements the NHibernate IUserType interface.
    /// Use this type for ARRAY<JSON> columns.
    /// </summary>
    public sealed class SpannerJsonArray : BaseSpannerArray<string>
    {
        public SpannerJsonArray()
        {
        }

        public SpannerJsonArray(List<string> array) : base(array)
        {
        }

        public override System.Type ReturnedType => typeof(SpannerJsonArray);

        public override object NullSafeGet(DbDataReader rs, string[] names, ISessionImplementor session, object owner) => 
            rs.IsDBNull(names[0]) ? null : new SpannerJsonArray(rs.GetFieldValue<List<string>>(names[0]));

        public override object DeepCopy(object value) => new SpannerJsonArray(new List<string>(Array));

        protected override SpannerDbType GetArrayElementType() => SpannerDbType.Json;
    }
}
