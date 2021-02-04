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

using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel
{
    public partial class Performance : VersionedEntity
    {
        public string VenueCode { get; set; }
        public DateTime ConcertStartTime { get; set; }
        public Guid SingerId { get; set; }
        public Guid AlbumId { get; set; }
        public long TrackId { get; set; }
        public DateTime? StartTime { get; set; }
        public double? Rating { get; set; }

        /// <summary>
        /// This timestamp is automatically filled by Cloud Spanner when a new row is inserted.
        /// <seealso cref="SpannerSampleDbContext"/> for the annotation that is attached to this property.
        /// </summary>
        public DateTime CreatedAt { get; set; }
        /// <summary>
        /// This timestamp is automatically filled by Cloud Spanner when a row is updated.
        /// <seealso cref="SpannerSampleDbContext"/> for the annotation that is attached to this property.
        /// </summary>
        public DateTime? LastUpdatedAt { get; set; }

        public virtual Concert Concert { get; set; }
        public virtual Singer Singer { get; set; }
        public virtual Track Track { get; set; }
    }
}
