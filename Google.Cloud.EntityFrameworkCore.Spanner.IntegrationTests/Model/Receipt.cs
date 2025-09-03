using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;

public struct Receipt
{
    public DateOnly Date { get; set; }
    
    public string Number { get; set; }
}