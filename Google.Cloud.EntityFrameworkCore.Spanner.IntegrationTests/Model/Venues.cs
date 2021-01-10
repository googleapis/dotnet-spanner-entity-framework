using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class Venues
    {
        public Venues()
        {
            Concerts = new HashSet<Concerts>();
        }

        public string Code { get; set; }
        public string Name { get; set; }
        public bool Active { get; set; }
        public long? Capacity { get; set; }
        public List<double> Ratings { get; set; }

        public virtual ICollection<Concerts> Concerts { get; set; }
    }
}
