using System.Data;

namespace Microsoft.EntityFrameworkCore.Storage.Internal
{
    //Note: This is required to customize the literals for 'true' and 'false' where
    // EFCore uses '1' and '0' by default.
    internal class SpannerBoolTypeMapping : BoolTypeMapping
    {
        public SpannerBoolTypeMapping(
            string storeType,
            DbType? dbType = null)
            : base(storeType, dbType)
        {
        }

        protected SpannerBoolTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        {
            return new SpannerBoolTypeMapping(parameters.StoreType, parameters.DbType);
        }

        protected override string GenerateNonNullSqlLiteral(object value)
            => (bool)value ? "true" : "false";
    }
}
