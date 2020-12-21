using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Update;
using System;
using System.Collections.Generic;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
    public class SpannerUpdateSqlGenerator : UpdateSqlGenerator
    {
        public SpannerUpdateSqlGenerator([NotNull] UpdateSqlGeneratorDependencies dependencies) : base(dependencies) { }


        protected override void AppendIdentityWhereCondition([NotNull] StringBuilder commandStringBuilder, [NotNull] ColumnModification columnModification)
        {
            commandStringBuilder.Append(" TRUE ");
        }

        protected override void AppendRowsAffectedWhereCondition([NotNull] StringBuilder commandStringBuilder, int expectedRowsAffected)
        {
            commandStringBuilder.Append(" TRUE ");
        }
    }
}
