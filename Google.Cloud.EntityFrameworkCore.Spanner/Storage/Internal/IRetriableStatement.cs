
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// Base interface for all statements that can be retried by SpannerRetriableTransaction.
    /// </summary>
    internal interface IRetriableStatement
    {
        internal Task Retry(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds);
    }
}
