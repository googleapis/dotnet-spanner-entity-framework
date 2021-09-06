﻿// Copyright 2020, Google Inc. All rights reserved.
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

namespace Google.Cloud.EntityFrameworkCore.Spanner.Metadata
{
    public static class SpannerAnnotationNames
    {
        /// <summary>
        /// Use this annotation for properties that should be automatically updated with the
        /// commit timestamp of the transaction. 
        /// </summary>
        public const string UpdateCommitTimestamp = "UpdateCommitTimestamp";
        
        /// <summary>
        /// Use this annotation for tables that should be interleaved in another table.
        /// </summary>
        public const string InterleaveInParent = "Spanner:InterleaveInParent";
        
        /// <summary>
        /// Annotation for setting the action to take for child records when a parent record
        /// is deleted.
        /// </summary>
        public const string InterleaveInParentOnDelete = "Spanner:InterleaveInParentOnDelete";
        
        /// <summary>
        /// Annotation for creating null-filtered indexes.
        /// </summary>
        public const string IsNullFilteredIndex = "Spanner:IsNullFiltered";
        
        /// <summary>
        /// Annotation for adding STORING columns to an index.
        /// </summary>
        public const string Storing = "Spanner:Storing";
    }

    public enum SpannerUpdateCommitTimestamp
    {
        /// <summary>
        /// Never update the property with the commit timestamp of the transaction.
        /// </summary>
        Never,
        /// <summary>
        /// Set the property to the commit timestamp of the transaction when the entity is updated.
        /// Use this to fill a `LastUpdatedOn` property.
        /// </summary>
        OnUpdate,
        /// <summary>
        /// Set the property to the commit timestamp of the transaction when the entity is inserted.
        /// Use this to fill a `CreatedOn` property.
        /// </summary>
        OnInsert,
        /// <summary>
        /// Set the property to the commit timestamp of the transaction when the entity is either inserted
        /// or updated. Use this fill a `CreatedOrLastUpdatedOn` property.
        /// </summary>
        OnInsertAndUpdate
    }

    public enum OnDelete
    {
        /// <summary>
        /// Cascade delete all child records when a parent record is deleted.
        /// </summary>
        Cascade,
        /// <summary>
        /// Do not take any action on the child records when a parent record is deleted.
        /// </summary>
        NoAction
    }
}
