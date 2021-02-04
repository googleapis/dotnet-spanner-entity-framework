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
using Google.Cloud.Spanner.Data;
using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples
{
    /// <summary>
    /// Main class for running a sample from the Snippets directory.
    /// Usage: `SampleRunner <DataSource> <SampleName>`
    /// Example: `SampleRunner projects/my-project/instances/my-instance/databases/my-database AddEntity`
    /// 
    /// The samples assume that the data model in 'SampleModel/SampleDataModel.sql' has already been created
    /// in the database. You can also execute `SampleRunner <DataSource> CreateDataModel` on an empty database
    /// to create all sample tables.
    /// </summary>
    public static class SampleRunner
    {
        static void Main(string[] args)
        {
            if (args == null || args.Length < 2)
            {
                Console.Error.WriteLine("Not enough arguments.\r\nUsage: RunSample <DataSource> <SampleName>\r\nExample: RunSample projects/my-project/instances/my-instance/databases/my-database AddEntity");
                return;
            }

            var dataSource = $"Data Source={args[0]}";
            var sampleName = args[1];
            RunSampleAsync(dataSource, sampleName).WaitWithUnwrappedExceptions();
        }

        private static async Task RunSampleAsync(string connectionString, string sampleName)
        {
            if (sampleName == "CreateSampleDataModel")
            {
                Console.WriteLine("Creating sample data model...");
                CreateSampleDataModel(connectionString);
                return;
            }

            Console.WriteLine($"Running sample {sampleName}");
            try
            {
                var sampleClass = System.Type.GetType($"Google.Cloud.EntityFrameworkCore.Spanner.Samples.Snippets.{sampleName}Sample");
                if (sampleClass == null)
                {
                    throw new ArgumentException($"Unknown sample name: {sampleName}");
                }
                var sampleMethod = sampleClass.GetMethod("Run");
                if (sampleMethod == null)
                {
                    throw new ArgumentException($"{sampleName} is not a valid sample as it does not contain a Run method");
                }
                var task = (Task) sampleMethod.Invoke(null, new[] { connectionString });
                await task.ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new ArgumentException($"Could not load sample {sampleName}. Please check that the sample name is a valid sample name.\r\nException: {e.Message}");
            }
        }

        private static void CreateSampleDataModel(string connectionString)
        {
            var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().CodeBase);
            var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
            var dirPath = Path.GetDirectoryName(codeBasePath);
            var fileName = Path.Combine(dirPath, "SampleModel/SampleDataModel.sql");
            Console.WriteLine(fileName);
            var script = File.ReadAllText(fileName);
            var statements = script.Split(";");
            for (var i = 0; i < statements.Length; i++)
            {
                statements[i] = statements[i].Trim(new char[] { '\r', '\n' });
            }
            int length = statements.Length;
            if (statements[length - 1] == "")
            {
                length--;
            }
            ExecuteDdl(connectionString, statements, length);
        }

        private static void ExecuteDdl(string connectionString, string[] ddl, int length)
        {
            string[] extraStatements = new string[length - 1];
            Array.Copy(ddl, 1, extraStatements, 0, extraStatements.Length);
            using var connection = new SpannerConnection(connectionString);
            connection.CreateDdlCommand(ddl[0].Trim(), extraStatements).ExecuteNonQuery();
        }
    }
}
