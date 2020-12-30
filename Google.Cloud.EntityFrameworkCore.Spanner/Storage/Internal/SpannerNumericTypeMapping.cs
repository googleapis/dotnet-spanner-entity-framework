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
    public class SpannerNumericTypeMapping : DecimalTypeMapping
    {
        public SpannerNumericTypeMapping()
            : base(SpannerDbType.Numeric.ToString()) { }

        public override RelationalTypeMapping Clone(string storeType, int? size) =>
            new SpannerNumericTypeMapping();

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        {
            return new SpannerNumericTypeMapping();
        }

        protected override void ConfigureParameter(DbParameter parameter)
        {
            ((SpannerParameter)parameter).SpannerDbType = SpannerDbType.Numeric;
            base.ConfigureParameter(parameter);
        }
    }
}
