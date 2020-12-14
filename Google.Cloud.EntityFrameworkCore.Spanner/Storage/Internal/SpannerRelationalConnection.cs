using Google.Cloud.Spanner.Data;
using System.Data.Common;

namespace Microsoft.EntityFrameworkCore.Storage.Internal
{
    public class SpannerRelationalConnection : RelationalConnection, ISpannerRelationalConnection
    {
        //Note: Wraps around a SpannerConnection.  It also sets up the log bridge for ADO.NET logs
        // to be seen in EF logs and has logic to set up a connection to the "master" db -- which in the spanner
        // world is simply a connection string that does not include a database.

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public SpannerRelationalConnection(RelationalConnectionDependencies dependencies)
            : base(dependencies)
        {
        }

        /// <inheritdoc />
        public override bool IsMultipleActiveResultSetsEnabled => true;

        protected override DbConnection CreateDbConnection() => new SpannerConnection(ConnectionString);

        /// <summary>
        /// </summary>
        public ISpannerRelationalConnection CreateMasterConnection()
        {
            var builder = new SpannerConnectionStringBuilder(ConnectionString);
            //Spanner actually has no master or admin db, so we just use a normal connection.
            var masterConn =
                new SpannerConnection($"Data Source=projects/{builder.Project}/instances/{builder.SpannerInstance}");
            var optionsBuilder = new DbContextOptionsBuilder();
            optionsBuilder.UseSpanner(masterConn);

            return new SpannerRelationalConnection(Dependencies.With(optionsBuilder.Options));
        }
    }
}
