using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class Addresses
    {
        public string Address { get; set; }
        public string Country { get; set; }
    }
}
