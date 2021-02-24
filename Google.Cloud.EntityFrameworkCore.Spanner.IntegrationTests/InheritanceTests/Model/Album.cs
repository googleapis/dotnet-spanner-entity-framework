using System;
using System.Collections.Generic;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.InheritanceTests.Model
{
    public partial class Album
    {
        public Album()
        {
        }

        public long AlbumId { get; set; }
        public string Title { get; set; }
        public SpannerDate? ReleaseDate { get; set; }

        public virtual Singer Singer { get; set; }
    }
}
