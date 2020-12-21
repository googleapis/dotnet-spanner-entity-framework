using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
    public class SpannerModificationCommandBatchFactory : IModificationCommandBatchFactory
    {
        private readonly ModificationCommandBatchFactoryDependencies _dependencies;
        private readonly IRelationalTypeMappingSource _typeMapper;

        public SpannerModificationCommandBatchFactory(
            [NotNull] ModificationCommandBatchFactoryDependencies dependencies,
            [NotNull] IRelationalTypeMappingSource typeMapper)
        {
            _dependencies = dependencies;
            _typeMapper = typeMapper;
        }

        public ModificationCommandBatch Create() => new SpannerModificationCommandBatch(_dependencies, _typeMapper);
    }
}
