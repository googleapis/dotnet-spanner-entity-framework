// Copyright 2025 Google LLC
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

using System.Reflection;
using Docker.DotNet.Models;
using Google.Api.Gax;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.DataProvider.GettingStartedGuide;

namespace Google.Cloud.Spanner.DataProvider;

/// <summary>
/// Main class for running a sample from the GettingStartedGuide directory.
/// Usage: `dotnet run SampleName DatabaseName`
/// Example: `dotnet run CreateConnection projects/my-project/instances/my-instance/databases/example-db`
/// </summary>
public static class SampleRunner
{
    private static readonly string SnippetsNamespace = typeof(CreateConnectionSample).Namespace!;
    
    static void Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.Error.WriteLine(string.Join(Environment.NewLine,
                "Not enough arguments.",
                "Usage: dotnet run <SampleName> <DatabaseName>",
                "Example: dotnet run CreateConnection projects/my-project/instances/my-instance/databases/example-db"));
            PrintValidSampleNames();
            return;
        }
        var sampleName = args[0];
        var databaseName = args[1];
        if (sampleName.Equals("All"))
        {
            // Run all samples. This is used to test that all samples are runnable.
            RunAllSamples(databaseName);
        }
        else
        {
            RunSample(sampleName, databaseName);
        }
    }

    private static void RunAllSamples(string databaseName)
    {
        var sampleClasses = GetSampleClasses();
        foreach (var sample in sampleClasses)
        {
            RunSample(GetSampleName(sample), databaseName);
        }
    }

    private static void RunSample(string sampleName, string databaseName)
    {
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
                var connectionString = $"Data Source={databaseName}";
                ((Task)sampleMethod.Invoke(null, [connectionString])!).WaitWithUnwrappedExceptions();
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"Running sample failed: {e.Message}\n{e.StackTrace}");
            throw;
        }
    }

    private static System.Type? GetSampleClass(string sampleName)
    {
        var sampleClass = System.Type.GetType($"{SnippetsNamespace}.{sampleName}Sample");
        if (sampleClass == null)
        {
            sampleClass = GetSampleClasses()
                .FirstOrDefault(t => string.Equals(GetSampleName(t), sampleName, StringComparison.OrdinalIgnoreCase));
        }
        return  sampleClass;
    }

    private static MethodInfo? GetSampleMethod(string sampleName)
    {
        try
        {
            var sampleClass = GetSampleClass(sampleName);
            if (sampleClass == null)
            {
                Console.Error.WriteLine($"Unknown sample name: {sampleName}");
                PrintValidSampleNames();
                return null;
            }
            var sampleMethods = sampleClass.GetMethods();
            var sampleMethod = sampleMethods.SingleOrDefault(m => m.GetParameters().Length == 1 && m.GetParameters()[0].ParameterType == typeof(string) && m.ReturnType == typeof(Task));
            if (sampleMethod == null)
            {
                Console.Error.WriteLine($"{sampleName} is not a valid sample as it does not contain a method with a single string argument that returns a Task.");
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

    private static string GetSampleName(System.Type sampleClass) => sampleClass.GetCustomAttribute<Sample>()!.Name;
    
    private static void PrintValidSampleNames()
    {
        var sampleClasses = GetSampleClasses();
        Console.Error.WriteLine("");
        Console.Error.WriteLine("Supported samples:");
        sampleClasses.ToList().ForEach(t => Console.Error.WriteLine($"  * {GetSampleName(t)}"));
    }
    
    private static IEnumerable<System.Type> GetSampleClasses()
        => Assembly.GetExecutingAssembly().GetTypes()
            .Where(t => t.IsClass && t.GetCustomAttribute<Sample>() is not null);
}
