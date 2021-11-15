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

using Google.Api.Gax;
using Google.Cloud.Spanner.Data;
using NHibernate.Engine;
using NHibernate.SqlTypes;
using NHibernate.UserTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Google.Cloud.NHibernate.Spanner
{
    /// <summary>
    /// A wrapper around a List that implements the NHibernate IUserType interface.
    /// Use this type for ARRAY<?> columns.
    /// </summary>
    public abstract class BaseSpannerArray<T> : IUserType, IEquatable<BaseSpannerArray<T>>
    {
        public List<T> Array { get; }

        public BaseSpannerArray()
        {
        }

        public BaseSpannerArray(List<T> array)
        {
            Array = GaxPreconditions.CheckNotNull(array, nameof(array));
        }

        public SqlType[] SqlTypes => new[] { new SqlType(DbType.Object) };
        public abstract System.Type ReturnedType { get; }
        public bool IsMutable => false;

        public override bool Equals(object other) => (other is BaseSpannerArray<T> bsa) && Equals(bsa);

        public bool Equals(BaseSpannerArray<T> other) => object.Equals(Array, other?.Array);

        public override int GetHashCode() => Array.GetHashCode();

        public override string ToString() => Array.ToString();

        public new bool Equals(object x, object y) => object.Equals(x, y);

        public int GetHashCode(object x) => x?.GetHashCode() ?? 0;
        
        public abstract object NullSafeGet(DbDataReader rs, string[] names, ISessionImplementor session, object owner);

        protected abstract SpannerDbType GetArrayElementType();

        public void NullSafeSet(DbCommand cmd, object value, int index, ISessionImplementor session)
        {
            if (cmd.Parameters[index] is SpannerParameter spannerParameter)
            {
                spannerParameter.SpannerDbType = SpannerDbType.ArrayOf(GetArrayElementType());
                if (value is BaseSpannerArray<T> bsa)
                {
                    spannerParameter.Value = bsa.Array;
                }
                else
                {
                    cmd.Parameters[index].Value = null;
                }
            }
        }

        public abstract object DeepCopy(object value);

        public object Replace(object original, object target, object owner) => original;

        public object Assemble(object cached, object owner) => cached;

        public object Disassemble(object value) => value;
    }
}
