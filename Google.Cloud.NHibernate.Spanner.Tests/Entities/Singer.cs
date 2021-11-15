using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;

namespace Google.Cloud.NHibernate.Spanner.Tests.Entities
{
    public class Singer
    {
        public virtual long SingerId { get; set; }
        
        public virtual string FirstName { get; set; }
        
        public virtual string LastName { get; set; }
        
        public virtual string FullName { get; set; }
        
        public virtual SpannerDate BirthDate { get; set; }
        
        public virtual object Picture { get; set; }
    }

    public class SingerMapping : ClassMapping<Singer>
    {
        public SingerMapping()
        {
            Id(x => x.SingerId);
            Property(x => x.FirstName);
            Property(x => x.LastName);
            Property(x => x.FullName, mapper => mapper.Generated(PropertyGeneration.Always));
            Property(x => x.BirthDate);
            // Property(x => x.Picture);
        }
    }
}
