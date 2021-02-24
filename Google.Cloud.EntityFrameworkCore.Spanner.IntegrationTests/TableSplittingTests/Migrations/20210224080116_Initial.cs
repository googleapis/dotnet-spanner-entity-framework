using Microsoft.EntityFrameworkCore.Migrations;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.TableSplittingTests.Migrations
{
    public partial class Initial : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Orders",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false),
                    Status = table.Column<string>(nullable: true),
                    BillingAddress = table.Column<string>(nullable: true),
                    ShippingAddress = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Orders", x => x.Id);
                });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Orders");
        }
    }
}
