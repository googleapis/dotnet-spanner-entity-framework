using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class Albums
    {
        public Albums()
        {
            Tracks = new HashSet<Tracks>();
        }

        public long AlbumId { get; set; }
        public string Title { get; set; }
        public DateTime? ReleaseDate { get; set; }
        public long SingerId { get; set; }

        public virtual Singers Singer { get; set; }
        public virtual ICollection<Tracks> Tracks { get; set; }
    }
}
