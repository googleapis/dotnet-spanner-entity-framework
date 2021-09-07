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

using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage
{
    /// <summary>
    /// A local date without a time component or a timezone. A date does not specify a
    /// specific point in time or a specific 24 hour interval. Use this struct instead
    /// of <see cref="DateTime"/> for Spanner DATE and ARRAY&lt;DATE&gt; columns.
    /// 
    /// Columns with type <see cref="DateTime"/> are mapped to TIMESTAMP in Cloud Spanner.
    /// </summary>
    public readonly struct SpannerDate : IEquatable<SpannerDate>, IConvertible, IComparable<SpannerDate>
    {
        public int Year { get; }
        public int Month { get; }
        public int Day { get; }

        public SpannerDate(int year, int month, int day)
        {
            Year = year;
            Month = month;
            Day = day;
        }

        public static SpannerDate FromDateTime(DateTime dateTime) => new SpannerDate(dateTime.Year, dateTime.Month, dateTime.Day);

        public static SpannerDate Today => FromDateTime(DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Unspecified));

        public DateTime ToDateTime() => new DateTime(Year, Month, Day, 0, 0, 0, DateTimeKind.Unspecified);

        public int DayOfYear => ToDateTime().DayOfYear;

        public DayOfWeek DayOfWeek => ToDateTime().DayOfWeek;

        public SpannerDate AddYears(int years) => FromDateTime(ToDateTime().AddYears(years));

        public SpannerDate AddMonths(int months) => FromDateTime(ToDateTime().AddMonths(months));

        public SpannerDate AddDays(int days) => FromDateTime(ToDateTime().AddDays(days));

        public override bool Equals(object other) => (other is SpannerDate sd) && Equals(sd);

        public bool Equals(SpannerDate other) => Year == other.Year && Month == other.Month && Day == other.Day;

        public int CompareTo(SpannerDate other)
        {
            if (Year != other.Year) return Year.CompareTo(other.Year);
            if (Month != other.Month) return Month.CompareTo(other.Month);
            return Day.CompareTo(other.Day);
        }

        public static bool operator ==(SpannerDate lhs, SpannerDate rhs) => lhs.Equals(rhs);

        public static bool operator !=(SpannerDate lhs, SpannerDate rhs) => !lhs.Equals(rhs);

        public static bool operator >(SpannerDate lhs, SpannerDate rhs) => lhs.CompareTo(rhs) > 0;

        public static bool operator <(SpannerDate lhs, SpannerDate rhs) => lhs.CompareTo(rhs) < 0;

        public static bool operator >=(SpannerDate lhs, SpannerDate rhs) => lhs.CompareTo(rhs) >= 0;

        public static bool operator <=(SpannerDate lhs, SpannerDate rhs) => lhs.CompareTo(rhs) <= 0;

        public override int GetHashCode() => (Year, Month, Day).GetHashCode();

        public override string ToString() => string.Format("{0:D04}-{1:D02}-{2:D02}", Year, Month, Day);

        public TypeCode GetTypeCode() => TypeCode.Object;

        public bool ToBoolean(IFormatProvider provider) => throw new InvalidCastException();

        public byte ToByte(IFormatProvider provider) => throw new InvalidCastException();

        public char ToChar(IFormatProvider provider) => throw new InvalidCastException();

        public DateTime ToDateTime(IFormatProvider provider) => ToDateTime();

        public decimal ToDecimal(IFormatProvider provider) => throw new InvalidCastException();

        public double ToDouble(IFormatProvider provider) => throw new InvalidCastException();

        public short ToInt16(IFormatProvider provider) => throw new InvalidCastException();

        public int ToInt32(IFormatProvider provider) => throw new InvalidCastException();

        public long ToInt64(IFormatProvider provider) => throw new InvalidCastException();

        public sbyte ToSByte(IFormatProvider provider) => throw new InvalidCastException();

        public float ToSingle(IFormatProvider provider) => throw new InvalidCastException();

        public string ToString(IFormatProvider provider) => ToString();

        public object ToType(System.Type conversionType, IFormatProvider provider)
        {
            if (conversionType == typeof(DateTime)) return ToDateTime();
            if (conversionType == typeof(string)) return ToString();
            throw new InvalidCastException();
        }

        public ushort ToUInt16(IFormatProvider provider) => throw new InvalidCastException();

        public uint ToUInt32(IFormatProvider provider) => throw new InvalidCastException();

        public ulong ToUInt64(IFormatProvider provider) => throw new InvalidCastException();
    }
}
