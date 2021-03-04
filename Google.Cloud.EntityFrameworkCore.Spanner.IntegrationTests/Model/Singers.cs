using System.Collections.Generic;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class Singers
    {
        public Singers()
        {
            Albums = new HashSet<Albums>();
            Concerts = new HashSet<Concerts>();
            Performances = new HashSet<Performances>();
        }

        public long SingerId { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string FullName { get; set; }
        public SpannerDate? BirthDate { get; set; }
        public byte[] Picture { get; set; }

        public virtual ICollection<Albums> Albums { get; set; }
        public virtual ICollection<Concerts> Concerts { get; set; }
        public virtual ICollection<Performances> Performances { get; set; }
    }
}
