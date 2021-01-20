using System;
using System.Collections.Generic;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.MigrationTests.Migrations
{
    public partial class Initial : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "AllColTypes",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false),
                    ColShort = table.Column<short>(nullable: true),
                    ColInt = table.Column<int>(nullable: true),
                    ColLong = table.Column<long>(nullable: true),
                    ColByte = table.Column<byte>(nullable: true),
                    ColSbyte = table.Column<sbyte>(nullable: true),
                    ColULong = table.Column<ulong>(nullable: true),
                    ColUShort = table.Column<ushort>(nullable: true),
                    ColDecimal = table.Column<SpannerNumeric>(nullable: true),
                    ColUint = table.Column<uint>(nullable: true),
                    ColBool = table.Column<bool>(nullable: true),
                    ColDate = table.Column<DateTime>(nullable: true),
                    ColTimestamp = table.Column<DateTime>(nullable: true),
                    ColCommitTimestamp = table.Column<DateTime>(nullable: true)
                        .Annotation("UpdateCommitTimestamp", SpannerUpdateCommitTimestamp.OnInsertAndUpdate),
                    ColFloat = table.Column<float>(nullable: true),
                    ColDouble = table.Column<double>(nullable: true),
                    ColString = table.Column<string>(nullable: true),
                    ColGuid = table.Column<Guid>(nullable: true),
                    ColBytes = table.Column<byte[]>(nullable: true),
                    ColDecimalArray = table.Column<SpannerNumeric[]>(nullable: true),
                    ColDecimalList = table.Column<List<SpannerNumeric>>(nullable: true),
                    ColStringArray = table.Column<string[]>(nullable: true),
                    ColStringList = table.Column<List<string>>(nullable: true),
                    ColBoolArray = table.Column<bool[]>(nullable: true),
                    ColBoolList = table.Column<List<bool>>(nullable: true),
                    ColDoubleArray = table.Column<double[]>(nullable: true),
                    ColDoubleList = table.Column<List<double>>(nullable: true),
                    ColLongArray = table.Column<long[]>(nullable: true),
                    ColLongList = table.Column<List<long>>(nullable: true),
                    ColDateArray = table.Column<DateTime[]>(nullable: true),
                    ColDateList = table.Column<List<DateTime>>(nullable: true),
                    ColTimestampArray = table.Column<DateTime[]>(nullable: true),
                    ColTimestampList = table.Column<List<DateTime>>(nullable: true),
                    ColBytesArray = table.Column<byte[][]>(nullable: true),
                    ColBytesList = table.Column<List<byte[]>>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AllColTypes", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Authors",
                columns: table => new
                {
                    AuthorId = table.Column<long>(nullable: false),
                    FirstName = table.Column<string>(nullable: true),
                    LastName = table.Column<string>(nullable: true),
                    FullName = table.Column<string>(nullable: true, computedColumnSql: "(ARRAY_TO_STRING([FirstName, LastName], ' ')) STORED")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Authors", x => x.AuthorId);
                });

            migrationBuilder.CreateTable(
                name: "Categories",
                columns: table => new
                {
                    CategoryId = table.Column<long>(nullable: false),
                    CategoryName = table.Column<string>(nullable: true),
                    CategoryDescription = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Categories", x => x.CategoryId);
                });

            migrationBuilder.CreateTable(
                name: "Orders",
                columns: table => new
                {
                    OrderId = table.Column<long>(nullable: false),
                    OrderDate = table.Column<DateTime>(nullable: false),
                    ShippedDate = table.Column<DateTime>(nullable: false),
                    ShipVia = table.Column<string>(nullable: true),
                    Freight = table.Column<float>(nullable: false),
                    ShipName = table.Column<string>(nullable: true),
                    ShipAddress = table.Column<string>(nullable: true),
                    ShipCity = table.Column<string>(nullable: true),
                    ShipRegion = table.Column<string>(nullable: true),
                    ShipPostalCode = table.Column<string>(nullable: true),
                    ShipCountry = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Orders", x => x.OrderId);
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

            migrationBuilder.CreateTable(
                name: "Products",
                columns: table => new
                {
                    ProductId = table.Column<long>(nullable: false),
                    ProductName = table.Column<string>(maxLength: 50, nullable: false),
                    CategoryId = table.Column<long>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Products", x => x.ProductId);
                    table.ForeignKey(
                        name: "FK_Products_Categories_CategoryId",
                        column: x => x.CategoryId,
                        principalTable: "Categories",
                        principalColumn: "CategoryId",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "OrderDetails",
                columns: table => new
                {
                    ProductId = table.Column<long>(nullable: false),
                    OrderId = table.Column<long>(nullable: false),
                    UnitPrice = table.Column<float>(nullable: false),
                    Quantity = table.Column<int>(nullable: false),
                    Discount = table.Column<float>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_OrderDetails", x => new { x.OrderId, x.ProductId });
                    table.ForeignKey(
                        name: "FK_OrderDetails_Orders_OrderId",
                        column: x => x.OrderId,
                        principalTable: "Orders",
                        principalColumn: "OrderId",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_OrderDetails_Products_ProductId",
                        column: x => x.ProductId,
                        principalTable: "Products",
                        principalColumn: "ProductId",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_AllColTypes_ColDate_ColCommitTimestamp",
                table: "AllColTypes",
                columns: new[] { "ColDate", "ColCommitTimestamp" })
                .Annotation("Spanner:IsNullFiltered", true);

            migrationBuilder.CreateIndex(
                name: "IX_OrderDetails_ProductId",
                table: "OrderDetails",
                column: "ProductId");

            migrationBuilder.CreateIndex(
                name: "IX_Products_CategoryId",
                table: "Products",
                column: "CategoryId");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "AllColTypes");

            migrationBuilder.DropTable(
                name: "Articles");

            migrationBuilder.DropTable(
                name: "OrderDetails");

            migrationBuilder.DropTable(
                name: "Authors");

            migrationBuilder.DropTable(
                name: "Orders");

            migrationBuilder.DropTable(
                name: "Products");

            migrationBuilder.DropTable(
                name: "Categories");
        }
    }
}
