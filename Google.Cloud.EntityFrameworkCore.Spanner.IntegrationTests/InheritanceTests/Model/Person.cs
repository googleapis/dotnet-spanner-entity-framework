using System;
using System.Collections.Generic;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.InheritanceTests.Model
{
    public partial class Person
    {
        public Person()
        {
        }

        public long PersonId { get; set; }

        public string Name { get; set; }
    }
}
