// Copyright 2021 Google Inc. All Rights Reserved.
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
using System.Collections.Generic;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class Singers
    {
        public Singers()
        {
            Albums = new HashSet<Albums>();
            Concerts = new HashSet<Concerts>();
            Performances = new HashSet<Performances>();
        }

        public long SingerId { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string FullName { get; set; }
        public SpannerDate? BirthDate { get; set; }
        public byte[] Picture { get; set; }

        public virtual ICollection<Albums> Albums { get; set; }
        public virtual ICollection<Concerts> Concerts { get; set; }
        public virtual ICollection<Performances> Performances { get; set; }
    }
}
