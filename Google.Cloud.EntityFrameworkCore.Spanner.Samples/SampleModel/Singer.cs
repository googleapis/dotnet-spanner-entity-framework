using System;
using System.Collections.Generic;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel
{
    public partial class Singer : VersionedEntity
    {
        public Singer()
        {
            Albums = new HashSet<Album>();
        }

        public Guid SingerId { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string FullName { get; set; }
        public SpannerDate? BirthDate { get; set; }
        public byte[] Picture { get; set; }

        public virtual ICollection<Album> Albums { get; set; }
    }
}
