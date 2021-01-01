using System;
using System.Collections.Generic;
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
                    ColDecimal = table.Column<decimal>(nullable: true),
                    ColUint = table.Column<uint>(nullable: true),
                    ColBool = table.Column<bool>(nullable: true),
                    ColDate = table.Column<DateTime>(type: "DATE", nullable: true),
                    ColTimestamp = table.Column<DateTime>(nullable: true),
                    ColFloat = table.Column<float>(nullable: true),
                    ColDouble = table.Column<double>(nullable: true),
                    ColString = table.Column<string>(nullable: true),
                    ColGuid = table.Column<Guid>(nullable: true),
                    ColBytes = table.Column<byte[]>(nullable: true),
                    ColDecimalArray = table.Column<decimal[]>(nullable: true),
                    ColDecimalList = table.Column<List<decimal>>(nullable: true),
                    ColStringArray = table.Column<string[]>(nullable: true),
                    ColStringList = table.Column<List<string>>(nullable: true),
                    ColBoolArray = table.Column<bool[]>(nullable: true),
                    ColBoolList = table.Column<List<bool>>(nullable: true),
                    ColDoubleArray = table.Column<double[]>(nullable: true),
                    ColDoubleList = table.Column<List<double>>(nullable: true),
                    ColLongArray = table.Column<long[]>(nullable: true),
                    ColLongList = table.Column<List<long>>(nullable: true),
                    ColTimestampArray = table.Column<DateTime[]>(nullable: true),
                    ColTimestampList = table.Column<List<DateTime>>(nullable: true),
                    ColDateArray = table.Column<DateTime[]>(type: "ARRAY<DATE>", nullable: true),
                    ColDateList = table.Column<List<DateTime>>(type: "ARRAY<DATE>", nullable: true),
                    ColBytesArray = table.Column<byte[][]>(nullable: true),
                    ColBytesList = table.Column<List<byte[]>>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AllColTypes", x => x.Id);
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
                name: "OrderDetails");

            migrationBuilder.DropTable(
                name: "Orders");

            migrationBuilder.DropTable(
                name: "Products");

            migrationBuilder.DropTable(
                name: "Categories");
        }
    }
}
