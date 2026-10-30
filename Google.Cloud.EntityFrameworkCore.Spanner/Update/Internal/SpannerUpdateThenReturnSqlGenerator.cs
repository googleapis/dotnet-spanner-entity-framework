// Copyright 2024, Google Inc. All rights reserved.
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

using Microsoft.EntityFrameworkCore.Update;
using System;
using System.Collections.Generic;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
    internal class SpannerUpdateThenReturnSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : UpdateSqlGenerator(dependencies)
    {
        protected override void AppendReturningClause(
            StringBuilder commandStringBuilder,
            IReadOnlyList<IColumnModification> operations,
            string additionalValues = null)
        {
            // In base EF Core, additionalValues is passed as "1" for UPDATE or DELETE statements that have no
            // read operations to support multi-statement batching engines that verify concurrency by reading a row.
            // Spanner natively returns exact affected row counts via ExecuteNonQuery (RowCountExact) and does not
            // need or benefit from a fake "THEN RETURN 1" clause, which would unnecessarily convert standard DML
            // into a streaming query. We therefore omit the THEN RETURN clause whenever there are no columns to read.
            if (operations.Count <= 0)
                return;
            AppendJoin(commandStringBuilder.AppendLine().Append("THEN RETURN "), operations, SqlGenerationHelper, ((sb, o, helper) => helper.DelimitIdentifier(sb, o.ColumnName)));
            if (additionalValues == null)
                return;
            commandStringBuilder.Append(", ");
            commandStringBuilder.Append(additionalValues);
        }

        // Copied from StringBuilderExtensions
        private static void AppendJoin<T, TParam>(
            StringBuilder stringBuilder,
            IEnumerable<T> values,
            TParam param,
            Action<StringBuilder, T, TParam> joinAction,
            string separator = ", ")
        {
            bool flag = false;
            foreach (T obj in values)
            {
                joinAction(stringBuilder, obj, param);
                stringBuilder.Append(separator);
                flag = true;
            }
            if (flag)
            {
                stringBuilder.Length -= separator.Length;
            }
        }

    }
}
