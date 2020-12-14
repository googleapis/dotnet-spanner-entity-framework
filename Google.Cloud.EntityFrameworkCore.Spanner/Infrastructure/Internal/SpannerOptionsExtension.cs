using Google.Api.Gax;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Microsoft.EntityFrameworkCore.Infrastructure.Internal
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerOptionsExtension : RelationalOptionsExtension
    {
        private DbContextOptionsExtensionInfo _info;

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public SpannerOptionsExtension()
        {
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        protected SpannerOptionsExtension(SpannerOptionsExtension original)
            : base(original)
        {
        }

        public override DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        protected override RelationalOptionsExtension Clone()
            => new SpannerOptionsExtension(this);

        /// <inheritdoc />
        public override void ApplyServices(IServiceCollection services)
        {
            GaxPreconditions.CheckNotNull(services, nameof(services));
            services.AddEntityFrameworkSpanner();
        }

        private sealed class ExtensionInfo : RelationalExtensionInfo
        {
            private long? _serviceProviderHash;
            private string _logFragment;

            public ExtensionInfo(IDbContextOptionsExtension extension)
                : base(extension)
            {
            }

            private new SpannerOptionsExtension Extension
                => (SpannerOptionsExtension)base.Extension;

            public override bool IsDatabaseProvider => true;

            public override string LogFragment
            {
                get
                {
                    if (_logFragment != null)
                        return _logFragment;

                    var builder = new StringBuilder(base.LogFragment);

                    return _logFragment = builder.ToString();
                }
            }

            public override long GetServiceProviderHashCode()
            {
                if (_serviceProviderHash == null)
                {
                    _serviceProviderHash = (base.GetServiceProviderHashCode() * 397) ^ 0L;
                }

                return _serviceProviderHash.Value;
            }

            public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
                            => debugInfo["Spanner: "] = (0L).ToString(CultureInfo.InvariantCulture);
        }
    }
}
