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

using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel
{

    /// <summary>
    /// Tracks are INTERLEAVED in the parent table Albums. This means that each Track record
    /// will physically be stored together with its parent Album record.
    /// 
    /// This relationship is seen as a foreign key by Entity Framework, although it technically is not.
    /// </summary>
    [InterleaveInParent(typeof(Album), OnDelete.Cascade)]
    public partial class Track : VersionedEntity
    {
        public Track()
        {
            Performances = new HashSet<Performance>();
        }

        /// <summary>
        /// The PRIMARY KEY of an INTERLEAVED table must always contain all the columns
        /// of the PRIMARY KEY of the parent table, in addition to any additional key columns
        /// of the child table. https://cloud.google.com/spanner/docs/schema-and-data-model#creating-interleaved-tables
        /// </summary>
        public Guid AlbumId { get; set; }
        public long TrackId { get; set; }

        public string Title { get; set; }

        /// <summary>
        /// NUMERIC columns are mapped to <see cref="SpannerNumeric"/> by default.
        /// </summary>
        public SpannerNumeric? Duration { get; set; }

        /// <summary>
        /// ARRAY columns are mapped to <see cref="List{T}"/> by default. Note that both
        /// the column itself (i.e. the List) can be null, as well as each individual element
        /// in the array itself. Defining an ARRAY column as NOT NULL will make the column itself
        /// not nullable, but each array element could still be null.
        /// </summary>
        public List<string> LyricsLanguages { get; set; }
        public List<string> Lyrics { get; set; }

        /// <summary>
        /// Track is a child table of Album. Entity Framework sees this as a FOREIGN KEY and allows
        /// it to be treated as such.
        /// </summary>
        public virtual Album Album { get; set; }

        public virtual ICollection<Performance> Performances { get; set; }
    }
}
