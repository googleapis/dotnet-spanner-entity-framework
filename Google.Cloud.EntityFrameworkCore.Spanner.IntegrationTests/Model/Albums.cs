using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class Albums
    {
        public long AlbumId { get; set; }
        public string Title { get; set; }
        public DateTime? ReleaseDate { get; set; }
        public long Singer { get; set; }

        public virtual Singers SingerNavigation { get; set; }
    }
}
