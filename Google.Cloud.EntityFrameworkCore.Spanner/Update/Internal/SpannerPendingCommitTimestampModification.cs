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

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Update;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
    internal class SpannerPendingCommitTimestampColumnModification : ColumnModification
    {
        internal const string PendingCommitTimestampValue = "PENDING_COMMIT_TIMESTAMP()";

        internal SpannerPendingCommitTimestampColumnModification(ColumnModification modification, bool sensitiveLoggingEnabled)
            : base(modification.Entry, modification.Property, () => "", modification.IsRead, modification.IsWrite, modification.IsKey, modification.IsCondition, modification.IsConcurrencyToken, sensitiveLoggingEnabled)
        {
        }

        public override bool IsWrite => true;

        public override bool UseCurrentValueParameter => false;

        public override bool UseOriginalValueParameter => false;

        public override object Value { get => PendingCommitTimestampValue; set => base.Value = value; }
    }


    internal class SpannerPendingCommitTimestampModificationCommand : ModificationCommand
    {
        private readonly ModificationCommand _delegate;
        private readonly IReadOnlyList<ColumnModification> _columnModifications;

        internal SpannerPendingCommitTimestampModificationCommand(ModificationCommand cmd, bool sensitiveLoggingEnabled) : base(cmd.TableName, cmd.Schema, cmd.ColumnModifications, sensitiveLoggingEnabled)
        {
            _delegate = cmd;
            List<ColumnModification> columnModifications = new List<ColumnModification>(cmd.ColumnModifications.Count);
            foreach (ColumnModification columnModification in cmd.ColumnModifications)
            {
                if (IsCommitTimestampModification(columnModification))
                {
                    columnModifications.Add(new SpannerPendingCommitTimestampColumnModification(columnModification, sensitiveLoggingEnabled));
                }
                else
                {
                    columnModifications.Add(columnModification);
                }
            }
            _columnModifications = columnModifications.AsReadOnly();
        }

        public override IReadOnlyList<ColumnModification> ColumnModifications { get => _columnModifications; }

        public override EntityState EntityState => _delegate.EntityState;

        public override bool RequiresResultPropagation => _delegate.RequiresResultPropagation;

        internal static bool HasCommitTimestampColumn(ModificationCommand modificationCommand)
        {
            foreach (var columnModification in modificationCommand.ColumnModifications)
            {
                if (IsCommitTimestampModification(columnModification))
                {
                    return true;
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
