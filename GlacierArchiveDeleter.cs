using Amazon.Glacier.Transfer;

namespace GlacierTools
{
    class GlacierArchiveDeleter
    {
        public static void DeleteArchive(string vaultName, string archiveId, Amazon.RegionEndpoint awsRegion)
        {
            Logger.LogMessage($"Deleting archive '{archiveId}' from {vaultName}...");
            using (var manager = new ArchiveTransferManager(awsRegion))
            {
                manager.DeleteArchive(vaultName, archiveId);
            }
        }
    }
}
