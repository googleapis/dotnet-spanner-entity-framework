using Google.Cloud.Spanner.Data;
using System.Data.Common;

namespace Microsoft.EntityFrameworkCore.Storage.Internal
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
