using System;
using System.Collections.Generic;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests.Migrations
{
    public partial class Initial : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Singers",
                columns: table => new
                {
                    SingerId = table.Column<long>(nullable: false),
                    FirstName = table.Column<string>(maxLength: 200, nullable: true),
                    LastName = table.Column<string>(maxLength: 200, nullable: false),
                    FullName = table.Column<string>(maxLength: 400, nullable: false, computedColumnSql: "(COALESCE(FirstName || ' ', '') || LastName) STORED"),
                    BirthDate = table.Column<DateTime>(nullable: true),
                    Picture = table.Column<byte[]>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PRIMARY_KEY", x => x.SingerId);
                });

            migrationBuilder.CreateTable(
                name: "TableWithAllColumnTypes",
                columns: table => new
                {
                    ColInt64 = table.Column<long>(nullable: false),
                    ColFloat64 = table.Column<double>(nullable: true),
                    ColNumeric = table.Column<SpannerNumeric>(nullable: true),
                    ColBool = table.Column<bool>(nullable: true),
                    ColString = table.Column<string>(maxLength: 100, nullable: true),
                    ColStringMax = table.Column<string>(nullable: true),
                    ColChar = table.Column<char>(nullable: true),
                    ColBytes = table.Column<byte[]>(type: "BYTES(100)", nullable: true),
                    ColBytesMax = table.Column<byte[]>(nullable: true),
                    ColDate = table.Column<DateTime>(nullable: true),
                    ColTimestamp = table.Column<DateTime>(nullable: true),
                    ColCommitTS = table.Column<DateTime>(nullable: true)
                        .Annotation("UpdateCommitTimestamp", SpannerUpdateCommitTimestamp.OnInsertAndUpdate),
                    ColInt64Array = table.Column<List<Nullable<long>>>(nullable: true),
                    ColFloat64Array = table.Column<List<Nullable<double>>>(nullable: true),
                    ColNumericArray = table.Column<List<Nullable<SpannerNumeric>>>(nullable: true),
                    ColBoolArray = table.Column<List<Nullable<bool>>>(nullable: true),
                    ColStringArray = table.Column<List<string>>(type: "ARRAY<STRING(100)>", nullable: true),
                    ColStringMaxArray = table.Column<List<string>>(nullable: true),
                    ColBytesArray = table.Column<List<byte[]>>(type: "ARRAY<BYTES(100)>", nullable: true),
                    ColBytesMaxArray = table.Column<List<byte[]>>(nullable: true),
                    ColDateArray = table.Column<List<Nullable<DateTime>>>(nullable: true),
                    ColTimestampArray = table.Column<List<Nullable<DateTime>>>(nullable: true),
                    ColGuid = table.Column<Guid>(nullable: true),
                    ColComputed = table.Column<string>(nullable: true, computedColumnSql: "(ARRAY_TO_STRING(ColStringArray, ',')) STORED")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PRIMARY_KEY", x => x.ColInt64);
                });

            migrationBuilder.CreateTable(
                name: "Venues",
                columns: table => new
                {
                    Code = table.Column<string>(maxLength: 10, nullable: false),
                    Name = table.Column<string>(maxLength: 100, nullable: true),
                    Active = table.Column<bool>(nullable: false),
                    Capacity = table.Column<long>(nullable: true),
                    Ratings = table.Column<List<Nullable<double>>>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PRIMARY_KEY", x => x.Code);
                });

            migrationBuilder.CreateTable(
                name: "Albums",
                columns: table => new
                {
                    AlbumId = table.Column<long>(nullable: false),
                    Title = table.Column<string>(maxLength: 100, nullable: false),
                    ReleaseDate = table.Column<DateTime>(nullable: true),
                    SingerId = table.Column<long>(nullable: false),
                    MarketingBudget = table.Column<long>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PRIMARY_KEY", x => x.AlbumId);
                    table.ForeignKey(
                        name: "FK_Albums_Singers",
                        column: x => x.SingerId,
                        principalTable: "Singers",
                        principalColumn: "SingerId",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "Concerts",
                columns: table => new
                {
                    VenueCode = table.Column<string>(maxLength: 10, nullable: false),
                    StartTime = table.Column<DateTime>(nullable: false),
                    SingerId = table.Column<long>(nullable: false),
                    Title = table.Column<string>(maxLength: 200, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PRIMARY_KEY", x => new { x.VenueCode, x.StartTime, x.SingerId });
                    table.ForeignKey(
                        name: "FK_Concerts_Singers",
                        column: x => x.SingerId,
                        principalTable: "Singers",
                        principalColumn: "SingerId",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Concerts_Venues",
                        column: x => x.VenueCode,
                        principalTable: "Venues",
                        principalColumn: "Code",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "Tracks",
                columns: table => new
                {
                    AlbumId = table.Column<long>(nullable: false),
                    TrackId = table.Column<long>(nullable: false),
                    Title = table.Column<string>(maxLength: 200, nullable: false),
                    Duration = table.Column<SpannerNumeric>(nullable: true),
                    LyricsLanguages = table.Column<List<string>>(type: "ARRAY<STRING(2)>", nullable: true),
                    Lyrics = table.Column<List<string>>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PRIMARY_KEY", x => new { x.AlbumId, x.TrackId });
                    table.CheckConstraint("Chk_Languages_Lyrics_Length_Equal", "ARRAY_LENGTH(LyricsLanguages) = ARRAY_LENGTH(Lyrics)");
                    table.ForeignKey(
                        name: "PK_Albums",
                        column: x => x.AlbumId,
                        principalTable: "Albums",
                        principalColumn: "AlbumId",
                        onDelete: ReferentialAction.Restrict);
                })
                .Annotation("Spanner:InterleaveInParent", "Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests.Models.Albums")
                .Annotation("Spanner:InterleaveInParentOnDelete", OnDelete.NoAction);

            migrationBuilder.CreateTable(
                name: "Performances",
                columns: table => new
                {
                    VenueCode = table.Column<string>(maxLength: 10, nullable: false),
                    SingerId = table.Column<long>(nullable: false),
                    StartTime = table.Column<DateTime>(nullable: false),
                    ConcertStartTime = table.Column<DateTime>(nullable: false),
                    AlbumId = table.Column<long>(nullable: false),
                    TrackId = table.Column<long>(nullable: false),
                    Rating = table.Column<double>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PRIMARY_KEY", x => new { x.VenueCode, x.SingerId, x.StartTime });
                    table.ForeignKey(
                        name: "FK_Performances_Singers",
                        column: x => x.SingerId,
                        principalTable: "Singers",
                        principalColumn: "SingerId",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Performances_Tracks",
                        columns: x => new { x.AlbumId, x.TrackId },
                        principalTable: "Tracks",
                        principalColumns: new[] { "AlbumId", "TrackId" },
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Performances_Concerts",
                        columns: x => new { x.VenueCode, x.ConcertStartTime, x.SingerId },
                        principalTable: "Concerts",
                        principalColumns: new[] { "VenueCode", "StartTime", "SingerId" },
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateIndex(
                name: "AlbumsByAlbumTitle2",
                table: "Albums",
                column: "Title")
                .Annotation("Spanner:Storing", new[] { "MarketingBudget", "ReleaseDate" });

            migrationBuilder.CreateIndex(
                name: "Idx_Singers_FullName",
                table: "Singers",
                column: "FullName");

            migrationBuilder.CreateIndex(
                name: "IDX_TableWithAllColumnTypes_ColDate_ColCommitTS",
                table: "TableWithAllColumnTypes",
                columns: new[] { "ColDate", "ColCommitTS" })
                .Annotation("Spanner:IsNullFiltered", true);

            migrationBuilder.CreateIndex(
                name: "Idx_Tracks_AlbumId_Title",
                table: "Tracks",
                columns: new[] { "TrackId", "Title" },
                unique: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Performances");

            migrationBuilder.DropTable(
                name: "TableWithAllColumnTypes");

            migrationBuilder.DropTable(
                name: "Tracks");

            migrationBuilder.DropTable(
                name: "Concerts");

            migrationBuilder.DropTable(
                name: "Albums");

            migrationBuilder.DropTable(
                name: "Venues");

            migrationBuilder.DropTable(
                name: "Singers");
        }
    }
}
