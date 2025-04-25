// Copyright 2020 Google Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.Spanner.Data;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    public class BasicsTests : IClassFixture<SingleTableFixture>
    {
        private readonly SingleTableFixture _fixture;

        public BasicsTests(SingleTableFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task CanInsertOrUpdateData()
        {
            await using var con = _fixture.GetConnection();
            await con.RunWithRetriableTransactionAsync(async (tx) =>
            {
                var cmd = con.CreateInsertOrUpdateCommand("TestTable", new SpannerParameterCollection
                {
                    new SpannerParameter { ParameterName = "Key", Value = "K1" },
                    new SpannerParameter { ParameterName = "Value", Value = "V1" },
                });
                cmd.Transaction = tx;
                await cmd.ExecuteNonQueryAsync();
            });
        }
    }
}
