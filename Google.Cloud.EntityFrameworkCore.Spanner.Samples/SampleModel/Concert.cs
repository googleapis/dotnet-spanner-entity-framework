﻿// Copyright 2021 Google LLC
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
    public partial class Concert : VersionedEntity
    {
        public Concert()
        {
            Performances = new HashSet<Performance>();
            TicketSales = new HashSet<TicketSale>();
        }

        public string VenueCode { get; set; }

        /// <summary>
        /// TIMESTAMP columns are mapped by default to <see cref="DateTime"/>. This distinguishes
        /// them from DATE columns that by default are mapped to <see cref="SpannerDate"/>.
        /// </summary>
        public DateTime StartTime { get; set; }
        public long SingerId { get; set; }
        public string Title { get; set; }

        public virtual Singer Singer { get; set; }
        public virtual Venue Venue { get; set; }

        public virtual ICollection<Performance> Performances { get; set; }

        public virtual ICollection<TicketSale> TicketSales { get; set; }
    }
}
