// Copyright 2025, Google Inc. All rights reserved.
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

using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerDateOnlyTypeMapping : DateOnlyTypeMapping
    {
        public new static SpannerDateOnlyTypeMapping Default { get; } = new();
        private static readonly ValueConverter s_converter = new ValueConverter<DateOnly, DateTime>(
            v => v.ToDateTime(TimeOnly.MinValue),
            v => DateOnly.FromDateTime(v));
        
        public SpannerDateOnlyTypeMapping()
            : base(new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(DateOnly), 
                    s_converter, 
                    jsonValueReaderWriter: JsonDateOnlyReaderWriter.Instance),
                "DATE", StoreTypePostfix.None, System.Data.DbType.Date))
        { }

        protected SpannerDateOnlyTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters) { }
    }
}
