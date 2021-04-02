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

using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Update;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
    /// <summary>
    /// This class is used for ColumnModifications that will set the value of the column to the commit timestamp of the transaction.
    /// These modifications must set the column value to a specific placeholder value.
    /// </summary>
    internal class SpannerPendingCommitTimestampColumnModification : ColumnModification
    {
        internal const string PendingCommitTimestampValue = "PENDING_COMMIT_TIMESTAMP()";

        internal SpannerPendingCommitTimestampColumnModification(IUpdateEntry entry, IProperty property, bool sensitiveLoggingEnabled)
            : base(entry, property, () => "", false, true, false, false, false, sensitiveLoggingEnabled)
        {
        }

        /// <summary>
        /// Indicates whether this modification will use mutations (true) or DML (false).
        /// </summary>
        internal bool IsMutationColumnModification { get; set; }

        public override bool IsWrite => true;

        public override bool UseCurrentValueParameter => IsMutationColumnModification;

        public override bool UseOriginalValueParameter => false;

        public override object Value
        {
            get => IsMutationColumnModification ? SpannerParameter.CommitTimestamp : PendingCommitTimestampValue;
            set => base.Value = value;
        }
    }

    /// <summary>
    /// ModificationCommand that is used for modifications that contain at least one ColumnModification that will
    /// set the value to the commit timestamp of the transaction.
    /// </summary>
    internal class SpannerPendingCommitTimestampModificationCommand : ModificationCommand
    {
        private readonly ModificationCommand _delegate;
        private readonly IReadOnlyList<ColumnModification> _columnModifications;

        internal SpannerPendingCommitTimestampModificationCommand(ModificationCommand cmd, bool sensitiveLoggingEnabled) : base(cmd.TableName, cmd.Schema, cmd.ColumnModifications, sensitiveLoggingEnabled)
        {
            _delegate = cmd;
            List<ColumnModification> columnModifications = new List<ColumnModification>(cmd.ColumnModifications.Count);
            foreach (var entry in cmd.Entries)
            {
                foreach (var prop in entry.EntityType.GetProperties())
                {
                    if (IsCommitTimestampModification(entry, prop))
                    {
                        columnModifications.Add(new SpannerPendingCommitTimestampColumnModification(entry, prop, sensitiveLoggingEnabled));
                    }
                }
            }
            foreach (ColumnModification columnModification in cmd.ColumnModifications)
            {
                if (!IsCommitTimestampModification(columnModification))
                {
                    columnModifications.Add(columnModification);
                }
            }
            _columnModifications = columnModifications.AsReadOnly();
        }

        internal void MarkAsMutationCommand()
        {
            foreach (var col in _columnModifications)
            {
                if (col is SpannerPendingCommitTimestampColumnModification commitTimestampColumnModification)
                {
                    commitTimestampColumnModification.IsMutationColumnModification = true;
                }
            }
        }

        public override IReadOnlyList<ColumnModification> ColumnModifications { get => _columnModifications; }

        public override EntityState EntityState => _delegate.EntityState;

        public override bool RequiresResultPropagation => _delegate.RequiresResultPropagation;

        internal static bool HasCommitTimestampColumn(ModificationCommand modificationCommand)
        {
            foreach (var entry in modificationCommand.Entries)
            {
                foreach (var prop in entry.EntityType.GetProperties())
                {
                    if (IsCommitTimestampModification(entry, prop))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        internal static bool IsCommitTimestampModification(IUpdateEntry entry, IProperty property)
        {
            if (property.FindAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp) != null)
            {
                if (property.FindAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp).Value is SpannerUpdateCommitTimestamp updateCommitTimestamp)
                {
                    switch (updateCommitTimestamp)
                    {
                        case SpannerUpdateCommitTimestamp.OnInsert:
                            return entry.EntityState == EntityState.Added;
                        case SpannerUpdateCommitTimestamp.OnUpdate:
                            return entry.EntityState == EntityState.Modified;
                        case SpannerUpdateCommitTimestamp.OnInsertAndUpdate:
                            return entry.EntityState == EntityState.Added || entry.EntityState == EntityState.Modified;
                        case SpannerUpdateCommitTimestamp.Never:
                        default:
                            return false;
                    }
                }
            }
            return false;
        }

        internal static bool IsCommitTimestampModification(ColumnModification columnModification)
        {
            if (columnModification.Property.FindAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp) != null)
            {
                if (columnModification.Property.FindAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp).Value is SpannerUpdateCommitTimestamp updateCommitTimestamp)
                {
                    switch (updateCommitTimestamp)
                    {
                        case SpannerUpdateCommitTimestamp.OnInsert:
                            return columnModification.Entry.EntityState == EntityState.Added;
                        case SpannerUpdateCommitTimestamp.OnUpdate:
                            return columnModification.Entry.EntityState == EntityState.Modified;
                        case SpannerUpdateCommitTimestamp.OnInsertAndUpdate:
                            return columnModification.Entry.EntityState == EntityState.Added || columnModification.Entry.EntityState == EntityState.Modified;
                        case SpannerUpdateCommitTimestamp.Never:
                        default:
                            return false;
                    }
                }
            }
            return false;
        }
    }
}
