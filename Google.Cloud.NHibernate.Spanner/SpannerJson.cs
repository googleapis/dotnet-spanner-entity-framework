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
using NHibernate.SqlTypes;
using NHibernate.UserTypes;
using System;
using System.Data;
using System.Data.Common;

namespace Google.Cloud.NHibernate.Spanner
{
    /// <summary>
    /// A wrapper around a string that implements the NHibernate IUserType interface.
    /// Use this type for JSON columns.
    /// </summary>
    public sealed class SpannerJson : IUserType, IEquatable<SpannerJson>
    {
        public string Json { get; }

        public SpannerJson()
        {
        }

        public SpannerJson(string json)
        {
            Json = json;
        }

        public SqlType[] SqlTypes => new[] { new SpannerSqlType(SpannerDbType.Json) };
        public System.Type ReturnedType => typeof(SpannerJson);
        public bool IsMutable => false;

        public override bool Equals(object other) => (other is SpannerJson sd) && Equals(sd);

        public bool Equals(SpannerJson other) => object.Equals(Json, other?.Json);

        public override int GetHashCode() => Json?.GetHashCode() ?? 0;

        public override string ToString() => Json ?? "null";

        bool IUserType.Equals(object x, object y) => object.Equals(x, y);

        public int GetHashCode(object x) => x?.GetHashCode() ?? 0;

        public object NullSafeGet(DbDataReader rs, string[] names, ISessionImplementor session, object owner) => 
            rs.IsDBNull(names[0]) ? null : new SpannerJson(rs.GetFieldValue<string>(names[0]));

        public void NullSafeSet(DbCommand cmd, object value, int index, ISessionImplementor session)
        {
            if (cmd.Parameters[index] is SpannerParameter spannerParameter)
            {
                spannerParameter.SpannerDbType = SpannerDbType.Json;
                if (value is SpannerJson spannerJson)
                {
                    spannerParameter.Value = spannerJson.Json;
                }
                else
                {
                    cmd.Parameters[index].Value = null;
                }
            }
        }

        public object DeepCopy(object value)
        {
            if (value is SpannerJson spannerJson)
            {
                return new SpannerJson(spannerJson.Json);
            }
            return null;
        }

        public object Replace(object original, object target, object owner) => original;

        public object Assemble(object cached, object owner) => cached;

        public object Disassemble(object value) => value;
    }
}
