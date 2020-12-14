using System.Data.Common;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Scaffolding.Internal
{
    public static class SpannerDataReaderExtension
    {
        public static T GetValueOrDefault<T>(this DbDataReader reader, string name)
        {
            var idx = reader.GetOrdinal(name);
            return reader.IsDBNull(idx)
                ? default
                : reader.GetFieldValue<T>(idx);
        }

        public static T GetValueOrDefault<T>(this DbDataRecord record, string name)
        {
            var idx = record.GetOrdinal(name);
            return record.IsDBNull(idx)
                ? default
                : (T)record.GetValue(idx);
        }
    }
}
