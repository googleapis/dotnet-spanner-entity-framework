// Copyright 2020 Google LLC
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

using Google.Cloud.Spanner.V1.Internal.Logging;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    public class SingleTableFixture : SpannerFixtureBase
    {
        public SingleTableFixture()
        {
            if (Database.Fresh)
            {
                Logger.DefaultLogger.Debug($"Creating database {Database.DatabaseName}");
                CreateTable();
            }
            else
            {
                Logger.DefaultLogger.Debug($"Deleting data in {Database.DatabaseName}");
                ClearTables();
            }
            Logger.DefaultLogger.Debug($"Ready to run tests");
        }

        private void ClearTables()
        {
            using var con = GetConnection();
            using var tx = con.BeginTransaction();
            var cmd = con.CreateDmlCommand("DELETE FROM TestTable WHERE TRUE");
            cmd.Transaction = tx;
            cmd.ExecuteNonQuery();
            tx.Commit();
        }

        private void CreateTable()
        {
            using var con = GetConnection();
            var cmd = con.CreateDdlCommand("CREATE TABLE TestTable (Key STRING(MAX), Value STRING(MAX)) PRIMARY KEY (Key)");
            cmd.ExecuteNonQuery();
        }
    }
}
