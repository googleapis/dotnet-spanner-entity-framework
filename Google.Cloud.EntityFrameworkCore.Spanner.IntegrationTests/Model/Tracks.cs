﻿// Copyright 2021 Google Inc. All Rights Reserved.
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

using System.Collections.Generic;
using Google.Cloud.Spanner.V1;
using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class Tracks
    {
        public Tracks()
        {
            Performances = new HashSet<Performances>();
        }

        public long AlbumId { get; set; }
        public long TrackId { get; set; }
        public string Title { get; set; }
        public SpannerNumeric? Duration { get; set; }
        public DateTime? RecordedAt { get; set; }
        public List<string> LyricsLanguages { get; set; }
        public List<string> Lyrics { get; set; }

        public virtual Albums Album { get; set; }
        public virtual ICollection<Performances> Performances { get; set; }
    }
}
