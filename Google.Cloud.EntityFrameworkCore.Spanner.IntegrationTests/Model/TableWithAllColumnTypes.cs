using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class TableWithAllColumnTypes
    {
        public long ColInt64 { get; set; }
        public double ColFloat64 { get; set; }
        public bool ColBool { get; set; }
        public string ColString { get; set; }
        public string ColStringMax { get; set; }
        public byte[] ColBytes { get; set; }
        public byte[] ColBytesMax { get; set; }
        public DateTime ColDate { get; set; }
        public DateTime ColTimestamp { get; set; }
        public DateTime ColCommitTs { get; set; }
        public List<long> ColInt64Array { get; set; }
        public List<double> ColFloat64Array { get; set; }
        public List<bool> ColBoolArray { get; set; }
        public List<string> ColStringArray { get; set; }
        public List<string> ColStringMaxArray { get; set; }
        public List<byte[]> ColBytesArray { get; set; }
        public List<byte[]> ColBytesMaxArray { get; set; }
        public List<DateTime> ColDateArray { get; set; }
        public List<DateTime> ColTimestampArray { get; set; }
        public string ColComputed { get; set; }
    }
}
