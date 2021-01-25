using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel
{
    public partial class Performance : VersionedEntity
    {
        public string VenueCode { get; set; }
        public DateTime ConcertStartTime { get; set; }
        public Guid SingerId { get; set; }
        public Guid AlbumId { get; set; }
        public long TrackId { get; set; }
        public DateTime? StartTime { get; set; }
        public double? Rating { get; set; }

        public virtual Concert Concert { get; set; }
        public virtual Singer Singer { get; set; }
        public virtual Track Track { get; set; }
    }
}
