// Copyright 2026 Google LLC
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

using System.Text.Json.Serialization;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;

/// <summary>
/// JSON type with property names containing special characters that require bracket notation
/// in JSON path expressions. This is used to test that VisitJsonScalar correctly handles
/// property names containing dots, spaces, quotes, and backslashes.
/// </summary>
public class JsonPropertiesWithSpecialNames
{
    /// <summary>
    /// Property with a dot in the JSON name - requires bracket notation $["property.with.dot"]
    /// </summary>
    [JsonPropertyName("property.with.dot")]
    public string PropertyWithDot { get; set; }

    /// <summary>
    /// Property with a space in the JSON name - requires bracket notation $["property with space"]
    /// </summary>
    [JsonPropertyName("property with space")]
    public string PropertyWithSpace { get; set; }

    /// <summary>
    /// Property with a single quote in the JSON name - requires bracket notation and quote escaping $["it's"]
    /// </summary>
    [JsonPropertyName("it's")]
    public string PropertyWithSingleQuote { get; set; }

    /// <summary>
    /// Property with double quotes in the JSON name - requires bracket notation and escaping $["say \"hello\""]
    /// </summary>
    [JsonPropertyName("say \"hello\"")]
    public string PropertyWithDoubleQuote { get; set; }

    /// <summary>
    /// Normal property without special characters - uses dot notation $.NormalProperty
    /// </summary>
    public string NormalProperty { get; set; }
}
