namespace Microsoft.EntityFrameworkCore.Storage.Internal
{
    public interface ISpannerRelationalConnection : IRelationalConnection
    {
        //Note: The relationalconnection classes represent an efcore level abstraction over the EFCore
        // providers.

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        ISpannerRelationalConnection CreateMasterConnection();
    }
}
