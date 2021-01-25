using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel
{

    [InterleaveInParent("Albums", OnDelete.Cascade)]
    public partial class Track : VersionedEntity
    {
        public Track()
        {
        }

        public Guid AlbumId { get; set; }
        public long TrackId { get; set; }
        public string Title { get; set; }
        public SpannerNumeric? Duration { get; set; }
        public List<string> LyricsLanguages { get; set; }
        public List<string> Lyrics { get; set; }

        public virtual Album Album { get; set; }
    }
}
