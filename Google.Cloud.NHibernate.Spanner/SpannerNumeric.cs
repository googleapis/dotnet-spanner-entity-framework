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
    /// A wrapper around a SpannerNumeric that implements the NHibernate IUserType interface.
    /// Use this type for NUMERIC columns.
    /// </summary>
    public sealed class SpannerNumeric : IUserType, IEquatable<SpannerNumeric>
    {
        public Cloud.Spanner.V1.SpannerNumeric Value { get; }

        public SpannerNumeric()
        {
        }

        public SpannerNumeric(Cloud.Spanner.V1.SpannerNumeric value)
        {
            Value = value;
        }

        public SqlType[] SqlTypes => new[] { new SqlType(DbType.VarNumeric) };
        public System.Type ReturnedType => typeof(Cloud.Spanner.V1.SpannerNumeric);
        public bool IsMutable => false;

        public override bool Equals(object other) => (other is SpannerNumeric sn) && Equals(sn);

        public bool Equals(SpannerNumeric other) => Equals(Value, other?.Value);

        public override int GetHashCode() => Value.GetHashCode();

        public override string ToString() => Value.ToString();

        bool IUserType.Equals(object x, object y) => Equals(x, y);

        public int GetHashCode(object x) => x?.GetHashCode() ?? 0;

        public object NullSafeGet(DbDataReader rs, string[] names, ISessionImplementor session, object owner) => 
            rs.IsDBNull(names[0]) ? null : new SpannerNumeric(rs.GetFieldValue<Cloud.Spanner.V1.SpannerNumeric>(names[0]));

        public void NullSafeSet(DbCommand cmd, object value, int index, ISessionImplementor session)
        {
            if (cmd.Parameters[index] is SpannerParameter spannerParameter)
            {
                spannerParameter.SpannerDbType = SpannerDbType.Numeric;
                if (value is SpannerNumeric spannerNumeric)
                {
                    spannerParameter.Value = spannerNumeric.Value;
                }
                else
                {
                    cmd.Parameters[index].Value = null;
                }
            }
        }

        public object DeepCopy(object value)
        {
            if (value is SpannerNumeric spannerNumeric)
            {
                return new SpannerNumeric(spannerNumeric.Value);
            }
            return null;
        }

        public object Replace(object original, object target, object owner) => original;

        public object Assemble(object cached, object owner) => cached;

        public object Disassemble(object value) => value;
    }
}
