// Copyright 2024 Google LLC
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

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel;

public class TicketSale : VersionedEntity
{
    /// <summary>
    /// This identifier is automatically generated by Spanner using a bit-reversed sequence.
    /// </summary>
    public long TicketSaleId;

    public string CustomerName { get; set; }
    
    public string[] Seats { get; set; }
    
    /// <summary>
    /// These properties combined form the reference to the Concert of this TicketSale.
    /// </summary>
    public string VenueCode { get; set; }
    public DateTime ConcertStartTime { get; set; }
    public Guid SingerId { get; set; }

    public virtual Concert Concert { get; set; }
}
