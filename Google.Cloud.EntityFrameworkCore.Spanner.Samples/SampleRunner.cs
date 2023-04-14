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
using Google.Api.Gax.ResourceNames;
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.Admin.Instance.V1;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples
{
    /// <summary>
    /// Main class for running a sample from the Snippets directory.
    /// Usage: `dotnet run SampleName`
    /// Example: `dotnet run AddEntitySample`
    /// 
    /// The SampleRunner will automatically start a docker container with a Spanner emulator and execute
    /// the sample on that emulator instance. No further setup or configuration is required.
    /// </summary>
    public static class SampleRunner
    {
        static void Main(string[] args)
        {
            if (args == null || args.Length < 1)
            {
                Console.Error.WriteLine("Not enough arguments.\r\nUsage: dotnet run <SampleName>\r\nExample: dotnet run AddEntitySample");
                PrintValidSampleNames();
                return;
            }
            var sampleName = args[0];
            if (sampleName.EndsWith("Sample"))
            {
                sampleName = sampleName.Substring(0, sampleName.Length - "Sample".Length);
            }
            try
            {
                var sampleMethod = GetSampleMethod(sampleName);
                if (sampleMethod != null)
                {
                    Console.WriteLine($"Running sample {sampleName}");
                    RunSampleAsync((connectionString) =>
                    {
                        return (Task)sampleMethod.Invoke(null, new object[] { connectionString });
                    }).WaitWithUnwrappedExceptions();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Running sample failed: {e.Message}");
            }
        }

        internal static async Task RunSampleAsync(Func<string, Task> sampleMethod)
        {
            Environment.SetEnvironmentVariable("SPANNER_EMULATOR_HOST", "localhost:9010");
            var emulatorRunner = new EmulatorRunner();
            try
            {
                Console.WriteLine("");
                Console.WriteLine("Starting emulator...");
                emulatorRunner.StartEmulator().WaitWithUnwrappedExceptions();
                Console.WriteLine("");

                var projectId = "sample-project";
                var instanceId = "sample-instance";
                var databaseId = "sample-database";
                DatabaseName databaseName = DatabaseName.FromProjectInstanceDatabase(projectId, instanceId, databaseId);
                var dataSource = $"Data Source={databaseName}";
                var connectionStringBuilder = new SpannerConnectionStringBuilder(dataSource)
                {
                    EmulatorDetection = EmulatorDetection.EmulatorOnly,
                };
                await MaybeCreateInstanceOnEmulatorAsync(databaseName.ProjectId, databaseName.InstanceId);
                await MaybeCreateDatabaseOnEmulatorAsync(databaseName);

                await sampleMethod.Invoke(connectionStringBuilder.ConnectionString);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Running sample failed: {e.Message}");
            }
            finally
            {
                Console.WriteLine("");
                Console.WriteLine("Stopping emulator...");
                emulatorRunner.StopEmulator().WaitWithUnwrappedExceptions();
                Console.WriteLine("");
            }
        }

        private static MethodInfo GetSampleMethod(string sampleName)
        {
            try
            {
                var sampleClass = System.Type.GetType($"{sampleName}Sample");
                if (sampleClass == null)
                {
                    Console.Error.WriteLine($"Unknown sample name: {sampleName}");
                    PrintValidSampleNames();
                    return null;
                }
                var sampleMethod = sampleClass.GetMethod("Run");
                if (sampleMethod == null)
                {
                    Console.Error.WriteLine($"{sampleName} is not a valid sample as it does not contain a Run method");
                    PrintValidSampleNames();
                    return null;
                }
                return sampleMethod;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"Could not load sample {sampleName}. Please check that the sample name is a valid sample name.\r\nException: {e.Message}");
                PrintValidSampleNames();
                return null;
            }
        }

        private static async Task MaybeCreateInstanceOnEmulatorAsync(string projectId, string instanceId)
        {
            // Try to create an instance on the emulator and ignore any AlreadyExists error.
            var adminClientBuilder = new InstanceAdminClientBuilder
            {
                EmulatorDetection = EmulatorDetection.EmulatorOnly
            };
            var instanceAdminClient = await adminClientBuilder.BuildAsync();

            var instanceName = InstanceName.FromProjectInstance(projectId, instanceId);
            try
            {
                await instanceAdminClient.CreateInstance(new CreateInstanceRequest
                {
                    InstanceId = instanceName.InstanceId,
                    ParentAsProjectName = ProjectName.FromProject(projectId),
                    Instance = new Instance
                    {
                        InstanceName = instanceName,
                        ConfigAsInstanceConfigName = new InstanceConfigName(projectId, "emulator-config"),
                        DisplayName = "Sample Instance",
                        NodeCount = 1,
                    },
                }).PollUntilCompletedAsync();
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.AlreadyExists)
            {
                // Ignore
            }
        }

        private static async Task MaybeCreateDatabaseOnEmulatorAsync(DatabaseName databaseName)
        {
            // Try to create a database on the emulator and ignore any AlreadyExists error.
            var adminClientBuilder = new DatabaseAdminClientBuilder
            {
                EmulatorDetection = EmulatorDetection.EmulatorOnly
            };
            var databaseAdminClient = await adminClientBuilder.BuildAsync();

            var instanceName = InstanceName.FromProjectInstance(databaseName.ProjectId, databaseName.InstanceId);
            try
            {
                await databaseAdminClient.CreateDatabase(new CreateDatabaseRequest
                {
                    ParentAsInstanceName = instanceName,
                    CreateStatement = $"CREATE DATABASE `{databaseName.DatabaseId}`",
                }).PollUntilCompletedAsync();
                var connectionStringBuilder = new SpannerConnectionStringBuilder($"Data Source={databaseName}")
                {
                    EmulatorDetection = EmulatorDetection.EmulatorOnly,
                };
                await CreateSampleDataModel(connectionStringBuilder.ConnectionString);
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.AlreadyExists)
            {
                // Ignore
            }
        }

        private static async Task CreateSampleDataModel(string connectionString)
        {
            var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().Location);
            var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
            var dirPath = Path.GetDirectoryName(codeBasePath);
            var fileName = Path.Combine(dirPath, "SampleModel/SampleDataModel.sql");
            var script = await File.ReadAllTextAsync(fileName);
            var statements = script.Split(";");
            for (var i = 0; i < statements.Length; i++)
            {
                // Remove license header from script
                if (statements[i].IndexOf("/*", StringComparison.Ordinal) >= 0 && statements[i].IndexOf("*/", StringComparison.Ordinal) >= 0)
                {
                    int startIndex = statements[i].IndexOf("/*", StringComparison.Ordinal);
                    int endIndex = statements[i].IndexOf("*/", startIndex, StringComparison.Ordinal) + "*/".Length;
                    statements[i] = statements[i].Remove(startIndex, endIndex - startIndex);
                }
                statements[i] = statements[i].Trim(new char[] { '\r', '\n' });
            }
            int length = statements.Length;
            if (statements[length - 1] == "")
            {
                length--;
            }
            await ExecuteDdlAsync(connectionString, statements, length);
        }

        private static async Task ExecuteDdlAsync(string connectionString, string[] ddl, int length)
        {
            string[] extraStatements = new string[length - 1];
            Array.Copy(ddl, 1, extraStatements, 0, extraStatements.Length);
            using var connection = new SpannerConnection(connectionString);
            await connection.CreateDdlCommand(ddl[0].Trim(), extraStatements).ExecuteNonQueryAsync();
        }

        private static void PrintValidSampleNames()
        {
            var sampleClasses = GetSampleClasses();
            Console.Error.WriteLine("");
            Console.Error.WriteLine("Supported samples:");
            sampleClasses.ToList().ForEach(t => Console.Error.WriteLine($"  * {t.Name}"));
        }

        private static IEnumerable<System.Type> GetSampleClasses()
            => from t in Assembly.GetExecutingAssembly().GetTypes()
                where t.IsClass && t.Name.EndsWith("Sample")
                select t;
    }
}
