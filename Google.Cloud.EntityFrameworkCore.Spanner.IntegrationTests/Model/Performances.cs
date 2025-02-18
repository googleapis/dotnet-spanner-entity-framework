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

using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public enum PerformanceType
    {
        Live,
        Playback,
    }

    public partial class Performances
    {
        public string VenueCode { get; set; }
        public DateTime ConcertStartTime { get; set; }
        public long SingerId { get; set; }
        public long AlbumId { get; set; }
        public long TrackId { get; set; }
        public DateTime? StartTime { get; set; }
        public double? Rating { get; set; }
        public PerformanceType PerformanceType { get; set; }

        public virtual Concerts Concerts { get; set; }
        public virtual Singers Singer { get; set; }
        public virtual Tracks Tracks { get; set; }
    }
}
