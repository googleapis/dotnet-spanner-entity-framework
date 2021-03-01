// Copyright 2020, Google Inc. All rights reserved.
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

namespace Microsoft.EntityFrameworkCore.Metadata
{
    public static class SpannerAnnotationNames
    {
        public const string UpdateCommitTimestamp = "UpdateCommitTimestamp";
        public const string InterleaveInParent = "Spanner:InterleaveInParent";
        public const string InterleaveInParentOnDelete = "Spanner:InterleaveInParentOnDelete";
        public const string IsNullFilteredIndex = "Spanner:IsNullFiltered";
        public const string Storing = "Spanner:Storing";
    }

    public enum SpannerUpdateCommitTimestamp
    {
        Never,
        OnUpdate,
        OnInsert,
        OnInsertAndUpdate
    }

    public enum OnDelete
    {
        Cascade,
        NoAction
    }
}
