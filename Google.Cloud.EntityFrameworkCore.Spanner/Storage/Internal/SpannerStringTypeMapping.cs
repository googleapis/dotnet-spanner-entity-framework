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
