using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.InheritanceTests.Model
{
    public partial class Singer : Person
    {
        public Singer()
        {
            Albums = new HashSet<Album>();
        }

        public string StageName { get; set; }

        public virtual ICollection<Album> Albums { get; set; }
    }
}
