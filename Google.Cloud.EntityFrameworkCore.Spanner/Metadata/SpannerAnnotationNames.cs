using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.EntityFrameworkCore.Metadata
{
    public static class SpannerAnnotationNames
    {
        public const string UpdateCommitTimestamp = "UpdateCommitTimestamp";
    }

    public enum SpannerUpdateCommitTimestamp
    {
        Never,
        OnUpdate,
        OnInsert,
        OnInsertAndUpdate
    }
}
