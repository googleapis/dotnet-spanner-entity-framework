using Google.Cloud.Spanner.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    public class SpannerRetriableCommand
    {
        private readonly SpannerCommand _spannerCommand;

        internal SpannerRetriableCommand(SpannerCommand spannerCommand)
        {
            _spannerCommand = spannerCommand;
        }
    }
}
