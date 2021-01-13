using System;
using System.Collections.Generic;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class Tracks
    {
        public Tracks()
        {
            Performances = new HashSet<Performances>();
        }

        public long AlbumId { get; set; }
        public long TrackId { get; set; }
        public string Title { get; set; }
        public SpannerNumeric? Duration { get; set; }
        public List<string> LyricsLanguages { get; set; }
        public List<string> Lyrics { get; set; }

        public virtual Albums Album { get; set; }
        public virtual ICollection<Performances> Performances { get; set; }
    }
}
