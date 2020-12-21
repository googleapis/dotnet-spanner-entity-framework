using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class Concerts
    {
        public Concerts()
        {
            Performances = new HashSet<Performances>();
        }

        public string VenueCode { get; set; }
        public DateTime StartTime { get; set; }
        public long SingerId { get; set; }
        public string Title { get; set; }

        public virtual Singers Singer { get; set; }
        public virtual Venues VenueCodeNavigation { get; set; }
        public virtual ICollection<Performances> Performances { get; set; }
    }
}
