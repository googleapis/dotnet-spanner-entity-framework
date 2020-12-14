// Copyright 2020, Google Inc. All rights reserved.
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

using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// Represents a complex spanner type mapping. This class is used for setting up type conversions.
    /// This class can be used to hold a type mapping for any <see cref="SpannerDbType"/>
    /// </summary>
    internal class SpannerComplexTypeMapping : RelationalTypeMapping
    {
        private readonly SpannerDbType _complexType;

        public SpannerComplexTypeMapping(SpannerDbType complexType)
            : base(complexType.ToString(), complexType.DefaultClrType, complexType.DbType) =>
            _complexType = complexType;

        public override RelationalTypeMapping Clone(string storeType, int? size) =>
            new SpannerComplexTypeMapping(_complexType);

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        {
            return new SpannerComplexTypeMapping(_complexType);
        }

        protected override void ConfigureParameter(DbParameter parameter)
        {
            // This key step will configure our SpannerParameter with this complex type, which will result in
            // the proper type conversions when the requests go out.
            ((SpannerParameter)parameter).SpannerDbType = _complexType;
            base.ConfigureParameter(parameter);
        }
    }
}
