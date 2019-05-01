using Amazon.Glacier.Transfer;
using Amazon.Runtime;
using System;

namespace GlacierTools
{
    class GlacierDownloader
    {
        static DateTime began = DateTime.MinValue;
        static long lastProgressReport = 0;

        private static void OnTransferProgress(object sender, StreamTransferProgressArgs e)
        {
            if (began == DateTime.MinValue)
                began = DateTime.Now;

            if (e.TransferredBytes - lastProgressReport < 8 * 1024 * 1024)
                return; // too soon to report again

            string rate = "<unknown>";
            int secondsElapsed = (int)DateTime.Now.Subtract(began).TotalSeconds;
            if (secondsElapsed > 0)
                rate = (((double)e.TransferredBytes / (double)(1024 * 1024)) / (double)secondsElapsed).ToString("0.##");

            string remainingTimeString = "";

            if (e.PercentDone > 0)
            {
                long elapsedSeconds = (long)DateTime.Now.Subtract(began).TotalSeconds;
                long totalSeconds = (long)((double)(elapsedSeconds) / (e.PercentDone / 100.0f));

                TimeSpan remainingTime = TimeSpan.FromSeconds(totalSeconds - elapsedSeconds);
                remainingTimeString = remainingTime.ToString();
            }

            Console.WriteLine("{0}: {1}% ({2} MB/s, {3} remaining)", DateTime.Now.ToString("HH:mm:ss"), e.PercentDone, rate, remainingTimeString);

            lastProgressReport = e.TransferredBytes;
        }

        public static void DownloadArchive(string vaultName, string archiveId, string outputPath, Amazon.RegionEndpoint awsRegion)
        {
            Logger.LogMessage($"Downloading archive '{archiveId}' from {vaultName}...");
            using (var manager = new ArchiveTransferManager(awsRegion))
            {
                var downloadOptions = new DownloadOptions() { PollingInterval = 1.0 };
                downloadOptions.StreamTransferProgress += OnTransferProgress;

                manager.Download(vaultName, archiveId, outputPath, downloadOptions);
            }
        }
    }
}
