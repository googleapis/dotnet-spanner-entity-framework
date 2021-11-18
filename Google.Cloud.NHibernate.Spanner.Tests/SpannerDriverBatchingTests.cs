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

using Google.Cloud.NHibernate.Spanner.Tests.Entities;
using Google.Cloud.Spanner.Connection.MockServer;
using Google.Protobuf.WellKnownTypes;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.NHibernate.Spanner.Tests
{
    public class SpannerDriverBatchingTests : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;

        public SpannerDriverBatchingTests(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        [CombinatorialData]
        [Theory]
        public async Task InsertMultipleAlbums_UsesBatchDml(bool async)
        {
            var insertSql = "INSERT INTO Album (Title, ReleaseDate, Singer, AlbumId) VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var album1 = new Album
            {
                AlbumId = 1L,
                Title = "Title 1",
            };
            var album2 = new Album
            {
                AlbumId = 2L,
                Title = "Title 2",
            };
            if (async)
            {
                await session.SaveAsync(album1);
                await session.SaveAsync(album2);
                await transaction.CommitAsync();
            }
            else
            {
                session.Save(album1);
                session.Save(album2);
                transaction.Commit();
            }
            AssertAlbumBatchDmlRequests(insertSql);
        }

        [CombinatorialData]
        [Theory]
        public async Task UpdateMultipleAlbums_UsesBatchDml(bool async)
        {
            var updateSql = "UPDATE Album SET Title = @p0, ReleaseDate = @p1, Singer = @p2 WHERE AlbumId = @p3";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));

            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = session.BeginTransaction();
            var album1 = new Album
            {
                AlbumId = 1L,
                Title = "Title 1",
            };
            var album2 = new Album
            {
                AlbumId = 2L,
                Title = "Title 2",
            };
            if (async)
            {
                await session.UpdateAsync(album1);
                await session.UpdateAsync(album2);
                await transaction.CommitAsync();
            }
            else
            {
                session.Update(album1);
                session.Update(album2);
                transaction.Commit();
            }
            AssertAlbumBatchDmlRequests(updateSql);
        }

        private void AssertAlbumBatchDmlRequests(string sql)
        {

            var transactionId = _fixture.SpannerMock.Requests.OfType<V1.CommitRequest>().First().TransactionId;
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteBatchDmlRequest>(),
                request =>
                {
                    Assert.Equal(transactionId, request.Transaction.Id);
                    Assert.Collection(request.Statements,
                        statement =>
                        {
                            Assert.Equal(sql, statement.Sql);
                            Assert.Collection(statement.Params.Fields,
                                param => Assert.Equal("Title 1", param.Value.StringValue),
                                param => Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase),
                                param => Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase),
                                param => Assert.Equal("1", param.Value.StringValue));
                        },
                        statement =>
                        {
                            Assert.Equal(sql, statement.Sql);
                            Assert.Collection(statement.Params.Fields,
                                param => Assert.Equal("Title 2", param.Value.StringValue),
                                param => Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase),
                                param => Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase),
                                param => Assert.Equal("2", param.Value.StringValue));
                        });
                });
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>());
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));
        }

        [CombinatorialData]
        [Theory]
        public async Task InsertMultipleAlbumsWithoutTransaction_UsesBatchDml(bool async)
        {
            // This scenario could in theory use Mutations instead of BatchDml. However, NHibernate is
            // not really made to create anything that is not SQL based. In this specific case it would
            // mean implementing a custom EntityPersister and override AbstractEntityPersister.GenerateInsertString.
            // That would mean overriding the heart of NHibernate, and is not worth the effort when compared to the
            // relatively small upside.
            
            var insertSql = "INSERT INTO Album (Title, ReleaseDate, Singer, AlbumId) VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

            using var session = _fixture.SessionFactory.OpenSession();
            var album1 = new Album
            {
                AlbumId = 1L,
                Title = "Title 1",
            };
            var album2 = new Album
            {
                AlbumId = 2L,
                Title = "Title 2",
            };
            if (async)
            {
                await session.SaveAsync(album1);
                await session.SaveAsync(album2);
                await session.FlushAsync();
            }
            else
            {
                session.Save(album1);
                session.Save(album2);
                session.Flush();
            }
            AssertAlbumBatchDmlRequests(insertSql);
        }
    }
}