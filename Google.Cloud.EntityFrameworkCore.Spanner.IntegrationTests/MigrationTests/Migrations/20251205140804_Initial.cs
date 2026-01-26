using System;
using System.Collections.Generic;
using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

#pragma warning disable CA1814 // Prefer jagged arrays over multidimensional

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.MigrationTests.Migrations
{
    /// <inheritdoc />
    public partial class Initial : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "AllColTypes",
                columns: table => new
                {
                    Id = table.Column<long>(type: "INT64", nullable: false)
                        .Annotation("Spanner:Identity", "'GeneratedByDefault', 'BIT_REVERSED_POSITIVE'"),
                    ColShort = table.Column<short>(type: "INT64", nullable: true),
                    ColInt = table.Column<int>(type: "INT64", nullable: true),
                    ColLong = table.Column<long>(type: "INT64", nullable: true),
                    ColByte = table.Column<byte>(type: "INT64", nullable: true),
                    ColSbyte = table.Column<sbyte>(type: "INT64", nullable: true),
                    ColULong = table.Column<ulong>(type: "INT64", nullable: true),
                    ColUShort = table.Column<ushort>(type: "INT64", nullable: true),
                    ColChar = table.Column<char>(type: "STRING(1)", nullable: true),
                    ColDecimal = table.Column<SpannerNumeric>(type: "NUMERIC", nullable: true),
                    ColUint = table.Column<uint>(type: "INT64", nullable: true),
                    ColBool = table.Column<bool>(type: "BOOL", nullable: true),
                    ColDate = table.Column<DateTime>(type: "DATE", nullable: true),
                    ColTimestamp = table.Column<DateTime>(type: "TIMESTAMP", nullable: true),
                    ColCommitTimestamp = table.Column<DateTime>(type: "TIMESTAMP", nullable: true)
                        .Annotation("UpdateCommitTimestamp", SpannerUpdateCommitTimestamp.OnInsertAndUpdate),
                    ColFloat = table.Column<float>(type: "FLOAT32", nullable: true),
                    ColDouble = table.Column<double>(type: "FLOAT64", nullable: true),
                    ColString = table.Column<string>(type: "STRING", nullable: true),
                    ASC = table.Column<string>(type: "STRING", nullable: true),
                    ColGuid = table.Column<Guid>(type: "STRING(36)", nullable: true),
                    ColBytes = table.Column<byte[]>(type: "BYTES", nullable: true),
                    ColJson = table.Column<string>(type: "JSON", nullable: true),
                    ColDecimalArray = table.Column<SpannerNumeric[]>(type: "ARRAY<NUMERIC>", nullable: true),
                    ColDecimalList = table.Column<List<SpannerNumeric>>(type: "ARRAY<NUMERIC>", nullable: true),
                    ColStringArray = table.Column<string[]>(type: "ARRAY<STRING>", nullable: true),
                    ColStringList = table.Column<List<string>>(type: "ARRAY<STRING>", nullable: true),
                    ColBoolArray = table.Column<bool[]>(type: "ARRAY<BOOL>", nullable: true),
                    ColBoolList = table.Column<List<bool>>(type: "ARRAY<BOOL>", nullable: true),
                    ColDoubleArray = table.Column<double[]>(type: "ARRAY<FLOAT64>", nullable: true),
                    ColDoubleList = table.Column<List<double>>(type: "ARRAY<FLOAT64>", nullable: true),
                    ColLongArray = table.Column<long[]>(type: "ARRAY<INT64>", nullable: true),
                    ColLongList = table.Column<List<long>>(type: "ARRAY<INT64>", nullable: true),
                    ColDateArray = table.Column<DateTime[]>(type: "ARRAY<DATE>", nullable: true),
                    ColDateList = table.Column<List<DateTime>>(type: "ARRAY<DATE>", nullable: true),
                    ColTimestampArray = table.Column<DateTime[]>(type: "ARRAY<TIMESTAMP>", nullable: true),
                    ColTimestampList = table.Column<List<DateTime>>(type: "ARRAY<TIMESTAMP>", nullable: true),
                    ColBytesArray = table.Column<byte[][]>(type: "ARRAY<BYTES>", nullable: true),
                    ColBytesList = table.Column<List<byte[]>>(type: "ARRAY<BYTES>", nullable: true),
                    ColJsonArray = table.Column<string[]>(type: "ARRAY<JSON>", nullable: true),
                    ColJsonList = table.Column<List<string>>(type: "ARRAY<JSON>", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AllColTypes", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Authors",
                columns: table => new
                {
                    AuthorId = table.Column<long>(type: "INT64", nullable: false)
                        .Annotation("Spanner:Identity", "'GeneratedByDefault', 'BIT_REVERSED_POSITIVE'"),
                    FirstName = table.Column<string>(type: "STRING", nullable: true),
                    LastName = table.Column<string>(type: "STRING", nullable: true),
                    FullName = table.Column<string>(type: "STRING", nullable: true, computedColumnSql: "(ARRAY_TO_STRING([FirstName, LastName], ' ')) STORED")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Authors", x => x.AuthorId);
                });

            migrationBuilder.CreateTable(
                name: "Categories",
                columns: table => new
                {
                    CategoryId = table.Column<long>(type: "INT64", nullable: false)
                        .Annotation("Spanner:Identity", "'GeneratedByDefault', 'BIT_REVERSED_POSITIVE'"),
                    CategoryName = table.Column<string>(type: "STRING", nullable: true),
                    CategoryDescription = table.Column<string>(type: "STRING", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Categories", x => x.CategoryId);
                });

            migrationBuilder.CreateTable(
                name: "Orders",
                columns: table => new
                {
                    OrderId = table.Column<long>(type: "INT64", nullable: false)
                        .Annotation("Spanner:Identity", "'GeneratedByDefault', 'BIT_REVERSED_POSITIVE'"),
                    OrderDate = table.Column<DateTime>(type: "TIMESTAMP", nullable: false, defaultValueSql: "current_timestamp"),
                    ShippedDate = table.Column<DateTime>(type: "TIMESTAMP", nullable: false),
                    ShipVia = table.Column<string>(type: "STRING", nullable: true),
                    Freight = table.Column<float>(type: "FLOAT32", nullable: false),
                    ShipName = table.Column<string>(type: "STRING", nullable: true),
                    ShipAddress = table.Column<string>(type: "STRING", nullable: true),
                    ShipCity = table.Column<string>(type: "STRING", nullable: true),
                    ShipRegion = table.Column<string>(type: "STRING", nullable: true),
                    ShipPostalCode = table.Column<string>(type: "STRING", nullable: true),
                    ShipCountry = table.Column<string>(type: "STRING", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Orders", x => x.OrderId);
                });

            migrationBuilder.CreateTable(
                name: "Articles",
                columns: table => new
                {
                    AuthorId = table.Column<long>(type: "INT64", nullable: false),
                    ArticleId = table.Column<long>(type: "INT64", nullable: false),
                    PublishDate = table.Column<DateTime>(type: "TIMESTAMP", nullable: false),
                    ArticleTitle = table.Column<string>(type: "STRING", nullable: true),
                    ArticleContent = table.Column<string>(type: "STRING", nullable: true)
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
                .Annotation("Spanner:InterleaveInParent", "Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Author")
                .Annotation("Spanner:InterleaveInParentOnDelete", OnDelete.Cascade);

            migrationBuilder.CreateTable(
                name: "Products",
                columns: table => new
                {
                    ProductId = table.Column<long>(type: "INT64", nullable: false)
                        .Annotation("Spanner:Identity", "'GeneratedByDefault', 'BIT_REVERSED_POSITIVE'"),
                    ProductName = table.Column<string>(type: "STRING(50)", maxLength: 50, nullable: false),
                    CategoryId = table.Column<long>(type: "INT64", nullable: false)
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
                    ProductId = table.Column<long>(type: "INT64", nullable: false),
                    OrderId = table.Column<long>(type: "INT64", nullable: false),
                    UnitPrice = table.Column<float>(type: "FLOAT32", nullable: false),
                    Quantity = table.Column<int>(type: "INT64", nullable: false),
                    Discount = table.Column<float>(type: "FLOAT32", nullable: false)
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

            migrationBuilder.InsertData(
                table: "Authors",
                columns: new[] { "AuthorId", "FirstName", "LastName" },
                values: new object[,]
                {
                    { 1L, "Belinda", "Stiles" },
                    { 2L, "Kelly", "Houser" }
                });

            migrationBuilder.CreateIndex(
                name: "IX_AllColTypes_ColDate_ColCommitTimestamp",
                table: "AllColTypes",
                columns: new[] { "ColDate", "ColCommitTimestamp" })
                .Annotation("Spanner:IsNullFiltered", true);

            migrationBuilder.CreateIndex(
                name: "IX_OrderedIndex",
                table: "AllColTypes",
                columns: new[] { "ColBool", "ColCommitTimestamp", "ColDecimal", "ColGuid" },
                descending: new[] { false, true, false, true });
        }

        /// <inheritdoc />
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
