using System;
using System.Collections.Generic;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel
{
    public partial class Album : VersionedEntity
    {
        public Album()
        {
        }

        public Guid AlbumId { get; set; }
        public string Title { get; set; }
        public SpannerDate? ReleaseDate { get; set; }
        public Guid SingerId { get; set; }

        public virtual Singer Singer { get; set; }
    }
}
