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

using System.Collections.Generic;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests.Models
{
    public partial class Albums
    {
        public Albums()
        {
            Tracks = new HashSet<Tracks>();
        }

        public long AlbumId { get; set; }
        public string Title { get; set; }
        public SpannerDate? ReleaseDate { get; set; }
        public long SingerId { get; set; }

        public virtual Singers Singer { get; set; }
        public virtual ICollection<Tracks> Tracks { get; set; }
    }
}
