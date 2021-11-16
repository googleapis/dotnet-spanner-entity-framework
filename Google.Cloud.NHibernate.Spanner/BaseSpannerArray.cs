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
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Google.Cloud.NHibernate.Spanner
{
    /// <summary>
    /// A wrapper around a List that implements the NHibernate IUserType interface.
    /// Use this type for ARRAY<?> columns.
    /// </summary>
    public abstract class BaseSpannerArray<T> : IUserType, IEquatable<BaseSpannerArray<T>>, ISpannerType
    {
        public List<T> Array { get; }

        public BaseSpannerArray()
        {
        }

        public BaseSpannerArray(List<T> array)
        {
            Array = array;
        }

        public SqlType[] SqlTypes => new[] { new SpannerSqlType(GetSpannerDbType()) };
        public abstract System.Type ReturnedType { get; }
        public bool IsMutable => false;

        public override bool Equals(object other)
        {
            if (other is BaseSpannerArray<T> otherArray)
            {
                return Equals(otherArray);
            }
            if (other == null)
            {
                return Array == null;
            }
            return false;
        }

        public bool Equals(BaseSpannerArray<T> other)
        {
            if (other == null)
            {
                return Array == null;
            }
            if (Array == null && other.Array == null)
            {
                return true;
            }
            if (Array == null || other.Array == null)
            {
                return false;
            }
            return Array.SequenceEqual(other.Array);
        }

        bool IUserType.Equals(object x, object y)
        {
            if (x == null && y == null)
            {
                return true;
            }
            if (x is BaseSpannerArray<T> arrayX && y is BaseSpannerArray<T> arrayY)
            {
                return arrayX.Equals(arrayY);
            }
            if (x is BaseSpannerArray<T> {Array: null} && y == null)
            {
                return true;
            }
            if (y is BaseSpannerArray<T> {Array: null} && x == null)
            {
                return true;
            }
            return false;
        }

        public override int GetHashCode() => Array.GetHashCode();

        public override string ToString() => Array.ToString();

        public int GetHashCode(object x) => x?.GetHashCode() ?? 0;
        
        public abstract object NullSafeGet(DbDataReader rs, string[] names, ISessionImplementor session, object owner);

        protected abstract SpannerDbType GetArrayElementType();

        public SpannerDbType GetSpannerDbType() => SpannerDbType.ArrayOf(GetArrayElementType());

        public void NullSafeSet(DbCommand cmd, object value, int index, ISessionImplementor session)
        {
            if (cmd.Parameters[index] is SpannerParameter spannerParameter)
            {
                spannerParameter.SpannerDbType = GetSpannerDbType();
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
