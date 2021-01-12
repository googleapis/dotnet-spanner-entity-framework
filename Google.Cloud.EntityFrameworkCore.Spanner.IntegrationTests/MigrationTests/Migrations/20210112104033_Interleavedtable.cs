using System;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.MigrationTests.Migrations
{
    public partial class Interleavedtable : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Authors",
                columns: table => new
                {
                    AuthorId = table.Column<long>(nullable: false),
                    AutherName = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Authors", x => x.AuthorId);
                });

            migrationBuilder.CreateTable(
                name: "Articles",
                columns: table => new
                {
                    AuthorId = table.Column<long>(nullable: false),
                    ArticleId = table.Column<long>(nullable: false),
                    PublishDate = table.Column<DateTime>(nullable: false),
                    ArticleTitle = table.Column<string>(nullable: true),
                    ArticleContent = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Articles", x => new { x.AuthorId, x.ArticleId });
                    table.ForeignKey(
                        name: "FK_Articles_Authors_AuthorId",
                        column: x => x.AuthorId,
                        principalTable: "Authors",
                        principalColumn: "AuthorId",
                        onDelete: ReferentialAction.Cascade);
                })
                .Annotation("Spanner:InterleaveInParent", "Authors")
                .Annotation("Spanner:InterleaveInParentOnDelete", OnDelete.Cascade);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Articles");

            migrationBuilder.DropTable(
                name: "Authors");
        }
    }
}
