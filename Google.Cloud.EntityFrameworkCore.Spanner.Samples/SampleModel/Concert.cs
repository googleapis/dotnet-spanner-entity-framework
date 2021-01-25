using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel
{
    public partial class Concert : VersionedEntity
    {
        public Concert()
        {
        }

        public string VenueCode { get; set; }
        public DateTime StartTime { get; set; }
        public long SingerId { get; set; }
        public string Title { get; set; }

        public virtual Singer Singer { get; set; }
        public virtual Venue Venue { get; set; }
    }
}
