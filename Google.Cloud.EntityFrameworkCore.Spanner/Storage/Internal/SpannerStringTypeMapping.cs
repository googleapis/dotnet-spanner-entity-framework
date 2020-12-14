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
using System;
using System.Data;
using System.Data.Common;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    public class SpannerStringTypeMapping : StringTypeMapping
    {
        private const int StringMax = 10485760;

        private readonly SpannerDbType _sqlDbType;
        private readonly int _maxSpecificSize;

        public SpannerStringTypeMapping(
            string storeType = null,
            bool unicode = false,
            int? size = null,
            bool fixedLength = false,
            SpannerDbType sqlDbType = null,
            StoreTypePostfix? storeTypePostfix = null)
            : this(
                new RelationalTypeMappingParameters(
                    new CoreTypeMappingParameters(typeof(string)),
                    storeType,
                    storeTypePostfix ?? StoreTypePostfix.Size,
                    GetDbType(unicode, fixedLength),
                    unicode,
                    size,
                    fixedLength),
                sqlDbType)
        {
        }

        private static DbType? GetDbType(bool unicode, bool fixedLength) => unicode
            ? (fixedLength ? System.Data.DbType.String : (DbType?)null)
            : System.Data.DbType.AnsiString;

        protected SpannerStringTypeMapping(RelationalTypeMappingParameters parameters, SpannerDbType sqlDbType)
            : base(parameters)
        {
            _maxSpecificSize = parameters.Size ?? StringMax;
            _sqlDbType = sqlDbType;
        }


        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new SpannerStringTypeMapping(parameters, _sqlDbType);


        protected override void ConfigureParameter(DbParameter parameter)
        {
            var value = parameter.Value;
            var length = (value as string)?.Length;

            if (parameter is SpannerParameter sqlParameter)
            {
                sqlParameter.SpannerDbType = _sqlDbType;
            }

            parameter.Size = value == null || value == DBNull.Value || length != null && length <= _maxSpecificSize
                ? _maxSpecificSize
                : -1;
        }
    }
}
