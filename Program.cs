using System;

namespace GlacierTools
{
    class Program
    {
        public enum ActionMode
        {
            Unknown,
            GlacierUpload,
            GlacierDownload,
            GlacierInventory,
            GlacierDelete
        }

        public class CmdlineParameters
        {
            public ActionMode actionMode = ActionMode.Unknown;
            public string vaultName = null;
            public string archiveId = null;
            public Amazon.RegionEndpoint awsRegion = null;

            // Uploader parameters
            public string fileToUpload = null;

            // Inventory parameters
            public string jobId = null;
            public string outputPath = null;
        }

        static void Main(string[] args)
        {
            bool showCommandLine = false;

            CmdlineParameters parameters = new CmdlineParameters();

            try
            {
                
                try
                {
                    for (int i = 0; i < args.Length - 1; i++)
                    {
                        if (args[i].ToLower() == "-region".ToLower())
                        {
                            parameters.awsRegion = Amazon.RegionEndpoint.GetBySystemName(args[i + 1]);
                            i++;
                        }
                        else if (args[i].ToLower() == "-vault".ToLower())
                        {
                            parameters.vaultName = args[i + 1];
                            i++;
                        }
                        else if (args[i].ToLower() == "-archiveId".ToLower())
                        {
                            parameters.archiveId = args[i + 1];
                            i++;
                        }
                        if (args[i].ToLower() == "-file".ToLower())
                        {
                            parameters.fileToUpload = args[i + 1];
                            i++;
                        }
                        else if (args[i].ToLower() == "-outfile".ToLower())
                        {
                            parameters.outputPath = args[i + 1];
                            i++;
                        }
                        else if (args[i].ToLower() == "-mode".ToLower())
                        {
                            string tmpMode = args[i + 1].ToLower();
                            i++;

                            if (tmpMode == "upload".ToLower())
                                parameters.actionMode = ActionMode.GlacierUpload;
                            else if (tmpMode == "download".ToLower())
                                parameters.actionMode = ActionMode.GlacierDownload;
                            else if (tmpMode == "inventory".ToLower())
                                parameters.actionMode = ActionMode.GlacierInventory;
                            else if (tmpMode == "delete".ToLower())
                                parameters.actionMode = ActionMode.GlacierDelete;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogMessage(ex.ToString());
                }

                if (string.IsNullOrEmpty(parameters.vaultName))
                {
                    showCommandLine = true;
                    throw new Exception("vault not specified");
                }

                if (parameters.awsRegion == null)
                {
                    showCommandLine = true;
                    throw new Exception("aws region not specified");
                }

                if (parameters.actionMode == ActionMode.GlacierUpload)
                {
                    GlacierUploader.Upload(parameters.vaultName, parameters.fileToUpload, parameters.awsRegion);
                }
                else if (parameters.actionMode == ActionMode.GlacierInventory)
                {
                    if (string.IsNullOrEmpty(parameters.outputPath))
                    {
                        showCommandLine = true;
                        throw new Exception("output path not specified");
                    }

                    GlacierInventory.GetInventory(parameters.vaultName, parameters.jobId, parameters.outputPath, parameters.awsRegion);
                }
                else if (parameters.actionMode == ActionMode.GlacierDownload)
                {
                    if (string.IsNullOrEmpty(parameters.outputPath))
                    {
                        showCommandLine = true;
                        throw new Exception("output path not specified");
                    }

                    GlacierDownloader.DownloadArchive(parameters.vaultName, parameters.archiveId, parameters.outputPath, parameters.awsRegion);
                }
                else if (parameters.actionMode == ActionMode.GlacierDelete)
                {
                    GlacierArchiveDeleter.DeleteArchive(parameters.vaultName, parameters.archiveId, parameters.awsRegion);
                }
                else if (parameters.actionMode == ActionMode.Unknown)
                {
                    showCommandLine = true;
                    throw new Exception("mode not specified");
                }

            }
            catch (Exception ex)
            {
                Logger.LogMessage(ex.ToString());
            }

            if (showCommandLine)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("GlacierTools -region <region> -vault <vaultName> -mode <upload|download|inventory|delete>");
            }
            else
                Console.ReadLine();
        }
    }
}