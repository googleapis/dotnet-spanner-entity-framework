using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class Performances
    {
        public string VenueCode { get; set; }
        public DateTime ConcertStartTime { get; set; }
        public long SingerId { get; set; }
        public long AlbumId { get; set; }
        public long TrackId { get; set; }
        public DateTime? StartTime { get; set; }
        public double? Rating { get; set; }

        public virtual Concerts Concerts { get; set; }
        public virtual Singers Singer { get; set; }
        public virtual Tracks Tracks { get; set; }
    }
}
