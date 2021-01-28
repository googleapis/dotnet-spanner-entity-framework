// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Migrations
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public abstract class MigrationSqlGeneratorTestBase
    {
        protected static string EOL => Environment.NewLine;

        protected virtual string Sql { get; set; }

        [Fact]
        public virtual void CreateTableOperation()
        {
            Generate(new CreateTableOperation
            {
                Name = "Albums",
                Columns =
                     {
                        new AddColumnOperation
                        {
                            Name = "AlbumId",
                            Table = "Albums",
                            ClrType = typeof(long),
                            IsNullable = false
                        },
                        new AddColumnOperation
                        {
                            Name = "Title",
                            Table = "Albums",
                            ClrType = typeof(string),
                            IsNullable = false,
                        },
                        new AddColumnOperation
                        {
                            Name = "ReleaseDate",
                            Table = "Albums",
                            ClrType = typeof(SpannerDate),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "SingerId",
                            Table = "Albums",
                            ClrType = typeof(long),
                            IsNullable = false
                        }
                     },
                PrimaryKey = new AddPrimaryKeyOperation { Columns = new[] { "AlbumId" } },
                CheckConstraints = { new CreateCheckConstraintOperation { Name = "Chk_Title_Length_Equal", Sql = "CHARACTER_LENGTH(Title) > 0" } },
                ForeignKeys =
                     {
                        new AddForeignKeyOperation
                        {
                            Name = "FK_Albums_Singers",
                            Columns = new[] { "SingerId" },
                            PrincipalTable = "Singers",
                            PrincipalColumns = new[] { "SingerId" },
                        }
                     },
            });
        }

        [Fact]
        public virtual void CreateTableWithAllColTypes()
        {
            var colCommitTimestampColumn = new AddColumnOperation
            {
                Name = "ColCommitTimestamp",
                Table = "AllColTypes",
                ClrType = typeof(DateTime),
                IsNullable = true,
            };
            colCommitTimestampColumn.AddAnnotation("UpdateCommitTimestamp", SpannerUpdateCommitTimestamp.OnInsertAndUpdate);

            Generate(new CreateTableOperation
            {
                Name = "AllColTypes",
                Columns =
                    {
                        new AddColumnOperation
                        {
                            Name = "Id",
                            Table = "AllColTypes",
                            ClrType = typeof(int),
                            IsNullable = false
                        },
                        new AddColumnOperation
                        {
                            Name = "ColShort",
                            Table = "AllColTypes",
                            ClrType = typeof(short),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColLong",
                            Table = "AllColTypes",
                            ClrType = typeof(long),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColByte",
                            Table = "AllColTypes",
                            ClrType = typeof(byte),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColSbyte",
                            Table = "AllColTypes",
                            ClrType = typeof(sbyte),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColULong",
                            Table = "AllColTypes",
                            ClrType = typeof(ulong),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColUShort",
                            Table = "AllColTypes",
                            ClrType = typeof(ushort),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDecimal",
                            Table = "AllColTypes",
                            ClrType = typeof(SpannerNumeric),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColUint",
                            Table = "AllColTypes",
                            ClrType = typeof(uint),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBool",
                            Table = "AllColTypes",
                            ClrType = typeof(bool),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDate",
                            Table = "AllColTypes",
                            ClrType = typeof(SpannerDate),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColTimestamp",
                            Table = "AllColTypes",
                            ClrType = typeof(DateTime),
                            IsNullable = true
                        },
                        colCommitTimestampColumn,
                        new AddColumnOperation
                        {
                            Name = "ColFloat",
                            Table = "AllColTypes",
                            ClrType = typeof(float),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDouble",
                            Table = "AllColTypes",
                            ClrType = typeof(double),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColString",
                            Table = "AllColTypes",
                            ClrType = typeof(string),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColGuid",
                            Table = "AllColTypes",
                            ClrType = typeof(Guid),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBytes",
                            Table = "AllColTypes",
                            ClrType = typeof(byte[]),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDecimalArray",
                            Table = "AllColTypes",
                            ClrType = typeof(SpannerNumeric[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDecimalList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<SpannerNumeric>),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColStringArray",
                            Table = "AllColTypes",
                            ClrType = typeof(string[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColStringList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<string>),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBoolArray",
                            Table = "AllColTypes",
                            ClrType = typeof(bool[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBoolList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<bool>),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDoubleArray",
                            Table = "AllColTypes",
                            ClrType = typeof(double[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDoubleList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<double>),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColLongArray",
                            Table = "AllColTypes",
                            ClrType = typeof(long[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColLongList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<long>),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDateArray",
                            Table = "AllColTypes",
                            ClrType = typeof(SpannerDate[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDateList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<SpannerDate>),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColTimestampArray",
                            Table = "AllColTypes",
                            ClrType = typeof(DateTime[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColTimestampList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<DateTime>),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBytesArray",
                            Table = "AllColTypes",
                            ClrType = typeof(byte[][]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBytesList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<byte[]>),
                            IsNullable = true
                        }
                    },
                PrimaryKey = new AddPrimaryKeyOperation { Columns = new[] { "AlbumId" } },
            });
        }

        [Fact]
        public virtual void CreateTableOperation_no_key()
            => Generate(new CreateTableOperation
            {
                Name = "Anonymous",
                Columns =
                    {
                        new AddColumnOperation
                        {
                            Name = "Value",
                            Table = "Anonymous",
                            ClrType = typeof(int),
                            IsNullable = false
                        }
                    }
            });

        [Fact]
        public virtual void CreateIndexOperation()
            => Generate(
                modelBuilder => modelBuilder.Entity("Singer").Property<string>("FullName").IsRequired(),
                new CreateIndexOperation
                {
                    Name = "IX_Singer_FullName",
                    Table = "Singer",
                    Columns = new[] { "FullName" },
                });

        [Fact]
        public virtual void CreateIndexOperation_is_null_filtered()
        {
            var createIndexOperation = new CreateIndexOperation
            {
                Name = "IX_Singer_FullName",
                Table = "Singer",
                Columns = new[] { "FullName" },
            };
            createIndexOperation.AddAnnotation(SpannerAnnotationNames.IsNullFilteredIndex, true);
            Generate(
                modelBuilder => modelBuilder.Entity("Singer").Property<string>("FullName"),
                createIndexOperation);
        }

        [Fact]
        public virtual void CreateIndexOperation_is_unique()
        {
            Generate(
                modelBuilder => modelBuilder.Entity("Singer").Property<string>("FullName"),
                new CreateIndexOperation
                {
                    Name = "IX_Singer_FullName",
                    Table = "Singer",
                    Columns = new[] { "FullName" },
                    IsUnique = true
                });
        }

        [Fact]
        public virtual void AddColumOperation()
            => Generate(new AddColumnOperation
            {
                Table = "Singer",
                Name = "Name",
                ClrType = typeof(string),
                ColumnType = "STRING(30)",
                IsNullable = false
            });


        protected TestHelpers TestHelpers { get; }

        protected MigrationSqlGeneratorTestBase(TestHelpers testHelpers)
        {
            TestHelpers = testHelpers;
        }

        protected virtual void Generate(params MigrationOperation[] operation)
            => Generate(_ => { }, operation);

        protected virtual void Generate(Action<ModelBuilder> buildAction, params MigrationOperation[] operation)
        {
            var modelBuilder = TestHelpers.CreateConventionBuilder();

            modelBuilder.Model.RemoveAnnotation(CoreAnnotationNames.ProductVersion);

            buildAction(modelBuilder);

            var batch = TestHelpers.CreateContextServices().GetRequiredService<IMigrationsSqlGenerator>()
                .Generate(operation, modelBuilder.Model);

            Sql = string.Join(EOL, batch.Select(b => b.CommandText));
        }

        protected void AssertSql(string expected)
            => Assert.Equal(expected, Sql, ignoreLineEndingDifferences: true);
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
