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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using System;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel
{
    public partial class Album : VersionedEntity
    {
        public Album()
        {
            Tracks = new HashSet<Track>();
        }

        /// <summary>
        /// Primary key of the Album. Cloud Spanner does not support any server side key generation.
        /// The key value must therefore be set or generated client side. Monotonically increasing values
        /// are NOT recommended for PRIMARY KEYs: https://cloud.google.com/spanner/docs/schema-and-data-model#choosing_a_primary_key
        /// </summary>
        public Guid AlbumId { get; set; }
        public string Title { get; set; }

        /// <summary>
        /// DATE columns are best mapped to <see cref="SpannerDate"/>.
        /// This makes it easier to distinguish them from TIMESTAMP columns, which are mapped to <see cref="DateTime"/>.
        /// </summary>
        public SpannerDate? ReleaseDate { get; set; }

        /// <summary>
        /// FOREIGN KEY value referencing a Singer record.
        /// </summary>
        public Guid SingerId { get; set; }

        public virtual Singer Singer { get; set; }

        public virtual ICollection<Track> Tracks { get; set; }
    }
}
