// Copyright 2021 Google Inc. All Rights Reserved.
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

using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Samples.Snippets;
using System;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples
{
    /// <summary>
    /// Main class for running a sample from the Snippets directory.
    /// Usage: `RunSample <DataSource> <SampleName>`
    /// Example: `RunSample projects/my-project/instances/my-instance/databases/my-database AddEntity`
    /// 
    /// The samples assume that the data model in 'SampleModel/SampleDataModel.sql' has already been created
    /// in the database. You can also execute `RunSample <DataSource> CreateDataModel` on an empty database
    /// to create all sample tables.
    /// </summary>
    class RunSample
    {
        static void Main(string[] args)
        {
            if (args == null || args.Length < 2)
            {
                Console.Error.WriteLine("Not enough arguments.\r\nUsage: RunSample <DataSource> <SampleName>\r\nExample: RunSample projects/my-project/instances/my-instance/databases/my-database AddEntity");
                return;
            }

            var dataSource = args[0];
            var sampleName = args[1];
            RunSampleAsync(dataSource, sampleName).WaitWithUnwrappedExceptions();
        }

        private static async Task RunSampleAsync(string dataSource, string sampleName)
        {
            switch (sampleName)
            {
                case "AddEntity":
                    await AddEntitySample.AddEntity(dataSource);
                    break;
                default:
                    throw new ArgumentException($"Unknown sample name: {sampleName}");
            }
        }
    }
}
