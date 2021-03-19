// Copyright 2021, Google Inc. All rights reserved.
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

using OpenTelemetry.Trace;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions
{
    public class TracerProviderExtension
    {
        private TracerProviderExtension()
        {
        }

        public static readonly string SPAN_NAME_COMMIT = "SpannerEFCore.Commit";
        public static readonly string SPAN_NAME_SAVECHANGES = "SpannerEFCore.SaveChanges";
        public static readonly string SPAN_NAME_QUERY = "SpannerEFCore.Query";
        public static readonly string ATTRIBUTE_NAME_RETRYING = "SpannerEFCore.Retrying";
        public static Tracer GetTracer() => TracerProvider.Default.GetTracer("Google.Cloud.EntityFrameworkCore.Spanner");
    }
}
