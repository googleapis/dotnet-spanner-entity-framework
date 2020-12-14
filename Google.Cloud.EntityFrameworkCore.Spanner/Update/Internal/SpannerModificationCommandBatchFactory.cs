using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace Microsoft.EntityFrameworkCore.Update.Internal
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerModificationCommandBatchFactory : IModificationCommandBatchFactory
    {
        private readonly IDiagnosticsLogger<DbLoggerCategory.Database.Command> _logger;
        private readonly IRelationalTypeMappingSource _typeMapper;

        /// <summary>
        /// </summary>
        /// <param name="typeMapper"></param>
        /// <param name="logger"></param>
        public SpannerModificationCommandBatchFactory(IRelationalTypeMappingSource typeMapper,
            IDiagnosticsLogger<DbLoggerCategory.Database.Command> logger)
        {
            _typeMapper = typeMapper;
            _logger = logger;
        }

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public virtual ModificationCommandBatch Create()
            => new SpannerModificationCommandBatch(_typeMapper, _logger);
    }
}
