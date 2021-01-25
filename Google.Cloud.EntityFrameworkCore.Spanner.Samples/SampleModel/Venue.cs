using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel
{
    public partial class Venue : VersionedEntity
    {
        public Venue()
        {
        }

        public string Code { get; set; }
        public string Name { get; set; }
        public bool Active { get; set; }
    }
}
