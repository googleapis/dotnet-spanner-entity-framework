using NHibernate.Driver;
using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;

namespace Google.Cloud.NHibernate.Spanner.Tests.Entities
{
    public class Singer
    {
        public virtual long SingerId { get; set; }
        
        public virtual string FirstName { get; set; }
        
        public virtual string LastName { get; set; }
    }

    public class SingerMap : ClassMapping<Singer>
    {
        public SingerMap()
        {
            Id(x => x.SingerId);
            Property(x => x.FirstName);
            Property(x => x.LastName);
        }
    }
}
