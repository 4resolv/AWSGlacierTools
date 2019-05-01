using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Glacier;
using Amazon.Runtime;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;

namespace GlacierTools
{
    class GlacierUploader
    {
        private static string dynamoTableName = "GlacierArchives";

        static long fileSize = 0;
        static long transferredBytes = 0;
        static long lastProgressReport = 0;

        static DateTime began = DateTime.MinValue;
        private static void OnTransferProgress(object sender, StreamTransferProgressArgs e)
        {
            if (began == DateTime.MinValue)
                began = DateTime.Now;

            if (transferredBytes + e.TransferredBytes - lastProgressReport < 8 * 1024 * 1024)
                return; // too soon to report again

            string rate = "<unknown>";
            int secondsElapsed = (int)DateTime.Now.Subtract(began).TotalSeconds;
            if (secondsElapsed > 0)
                rate = (((double)transferredBytes / (double)(1024 * 1024)) / (double)secondsElapsed).ToString("0.##");

            double percentComplete = ((((double)transferredBytes + (double)e.TransferredBytes) * 100.0f) / (double)fileSize);

            string remainingTimeString = "";

            if (percentComplete > 0)
            {
                long elapsedSeconds = (long)DateTime.Now.Subtract(began).TotalSeconds;
                long totalSeconds = (long)((double)(elapsedSeconds) / (percentComplete / 100.0f));

                TimeSpan remainingTime = TimeSpan.FromSeconds(totalSeconds - elapsedSeconds);
                remainingTimeString = remainingTime.ToString();
            }

            Console.WriteLine("{0}: {1}% ({2} MB/s, {3} remaining)", DateTime.Now.ToString("HH:mm:ss"), percentComplete.ToString("0.##"), rate, remainingTimeString);

            lastProgressReport = transferredBytes + e.TransferredBytes;
        }

        static ConcurrentQueue<Tuple<byte[], int>> hashBuffers = new ConcurrentQueue<Tuple<byte[], int>>();
        static bool buffersDone = false;
        static bool hashComplete = false;

        private static void InitializeDynamo(Amazon.RegionEndpoint awsRegion, string dynamoTableName)
        {
            AmazonDynamoDBClient ddbClient = new AmazonDynamoDBClient(awsRegion);

            try
            {

                Table archivesTable = Table.LoadTable(ddbClient, dynamoTableName);
            }
            catch (ResourceNotFoundException)
            {
                CreateTableRequest createRequest = new CreateTableRequest();
                createRequest.TableName = dynamoTableName;
                createRequest.ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 1,
                    WriteCapacityUnits = 1
                };

                createRequest.KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement
                    {
                        AttributeName = "ArchiveId",
                        KeyType = "HASH"
                    },
                };

                createRequest.AttributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition
                    {
                        AttributeName = "ArchiveId",
                        AttributeType = "S"
                    },
                };

                CreateTableResponse response = ddbClient.CreateTable(createRequest);

                DescribeTableRequest descRequest = new DescribeTableRequest
                {
                    TableName = dynamoTableName
                };

                // Wait till table is active
                bool isActive = false;
                while (!isActive)
                {
                    Logger.LogMessage("Waiting for table to initialize...");
                    System.Threading.Thread.Sleep(5000);
                    DescribeTableResponse descResponse = ddbClient.DescribeTable(descRequest);
                    string tableStatus = descResponse.Table.TableStatus;

                    if (string.Equals(tableStatus, "Active", StringComparison.InvariantCultureIgnoreCase))
                        isActive = true;
                }

                Logger.LogMessage("Table initialized!");
            }
        }

        public static void Upload(string vaultName, string fileToUpload, Amazon.RegionEndpoint awsRegion)
        {
            try
            {
                if (string.IsNullOrEmpty(vaultName))
                    throw new Exception("No vault specified");

                if (string.IsNullOrEmpty(fileToUpload) || !File.Exists(fileToUpload))
                    throw new Exception($"Invalid file '{fileToUpload}'");

                // Verify that the dynamo table exists
                InitializeDynamo(awsRegion, dynamoTableName);

                string archiveName = Path.GetFileName(fileToUpload);

                fileSize = new FileInfo(fileToUpload).Length;

                using (var glacier = new AmazonGlacierClient(awsRegion))
                using (FileStream fs = File.OpenRead(fileToUpload))
                using (SHA256Managed sha = new SHA256Managed())
                {
                    // Do the SHA256 hash in a background worker to avoid blocking
                    ThreadPool.QueueUserWorkItem(unused =>
                    {
                        byte[] lastBuffer = new byte[] { };

                        while (!buffersDone || !hashBuffers.IsEmpty)
                        {
                            Tuple<byte[], int> chunkTohash;
                            if (hashBuffers.TryDequeue(out chunkTohash))
                            {
                                sha.TransformBlock(chunkTohash.Item1, 0, chunkTohash.Item2, null, 0);
                                lastBuffer = chunkTohash.Item1;
                            }

                            Thread.Sleep(10);
                        }

                        sha.TransformFinalBlock(lastBuffer, 0, 0);

                        hashComplete = true;
                    });

                    long partSize = 128 * 1024 * 1024;

                    var initUploadRequest = new Amazon.Glacier.Model.InitiateMultipartUploadRequest();
                    initUploadRequest.ArchiveDescription = archiveName;
                    initUploadRequest.PartSize = partSize;
                    initUploadRequest.VaultName = vaultName;

                    var initResponse = glacier.InitiateMultipartUpload(initUploadRequest);

                    long position = 0;

                    fs.Seek(0, SeekOrigin.Begin);

                    List<byte[]> treeHashes = new List<byte[]>();

                    while (true)
                    {
                        byte[] buffer = new byte[partSize];

                        int bytesRead = fs.Read(buffer, 0, (int)partSize);

                        if (bytesRead == 0)
                            break;

                        using (MemoryStream ms = new MemoryStream(buffer))
                        {
                            ms.Seek(0, SeekOrigin.Begin);
                            ms.SetLength(bytesRead);
                            byte[] treeHash = HashUtil.ComputeSHA256TreeHash(buffer, bytesRead);
                            treeHashes.Add(treeHash);

                            ms.Seek(0, SeekOrigin.Begin);

                            var uploadRequest = new Amazon.Glacier.Model.UploadMultipartPartRequest();
                            uploadRequest.Body = ms;
                            uploadRequest.UploadId = initResponse.UploadId;
                            uploadRequest.VaultName = vaultName;
                            uploadRequest.StreamTransferProgress += OnTransferProgress;

                            uploadRequest.Checksum = BitConverter.ToString(treeHash).Replace("-", "").ToLower();

                            long firstByte = position;
                            long lastByte = position + bytesRead - 1;

                            uploadRequest.Range = $"bytes {firstByte}-{lastByte}/{fileSize}";
                            var uploadResponse = glacier.UploadMultipartPart(uploadRequest);
                        }

                        hashBuffers.Enqueue(new Tuple<byte[], int>(buffer, bytesRead));

                        position += bytesRead;
                        transferredBytes += bytesRead;
                    }

                    buffersDone = true;
                    while (!hashComplete)
                    {
                        Thread.Sleep(10);
                    }

                    var completeUploadRequest = new Amazon.Glacier.Model.CompleteMultipartUploadRequest();
                    completeUploadRequest.ArchiveSize = fileSize.ToString();
                    completeUploadRequest.UploadId = initResponse.UploadId;
                    completeUploadRequest.VaultName = vaultName;

                    byte[] fullTreeHash = HashUtil.ComputeSHA256TreeHash(treeHashes.ToArray());

                    completeUploadRequest.Checksum = BitConverter.ToString(fullTreeHash).Replace("-", "").ToLower();
                    var completeUploadResponse = glacier.CompleteMultipartUpload(completeUploadRequest);


                    string fileHash = BitConverter.ToString(sha.Hash).Replace("-", String.Empty);
                    Console.WriteLine("File hash: " + fileHash);

                    WriteArchiveToDynamo(completeUploadResponse.ArchiveId, awsRegion, vaultName, archiveName, fileSize, fileHash, completeUploadResponse.Location);

                    Console.WriteLine("Copy and save the following Archive ID for the next step.");
                    Console.WriteLine("Archive ID: {0}", initResponse.UploadId);
                    Console.WriteLine("To continue, press Enter");
                    Console.ReadKey();
                }
            }
            catch (AmazonGlacierException e)
            {
                Console.WriteLine(e.Message);
            }
            catch (AmazonServiceException e)
            {
                Console.WriteLine(e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            Console.ReadKey();
        }

        public static void WriteArchiveToDynamo(string archiveId, Amazon.RegionEndpoint awsRegion, string vaultName, string fileName, long fileSize, string fileHash, string archiveUri)
        {
            Console.WriteLine("Saving archive to dynamodb...");

            int attempt = 0;
            while (true)
            {
                try
                {
                    AmazonDynamoDBClient ddbClient = new AmazonDynamoDBClient(awsRegion);

                    Table archivesTable = Table.LoadTable(ddbClient, dynamoTableName);

                    var archive = new Document();
                    archive["ArchiveId"] = archiveId;
                    archive["VaultName"] = vaultName;
                    archive["ArchiveDescription"] = fileName;
                    archive["ArchiveSize"] = fileSize;
                    archive["ArchiveHash"] = fileHash;
                    archive["ArchiveUri"] = archiveUri;
                    archive["Uploaded"] = DateTime.UtcNow.ToString("o", System.Globalization.CultureInfo.InvariantCulture);

                    archivesTable.PutItem(archive);
                    return;
                }
                catch (ProvisionedThroughputExceededException)
                {
                    attempt++;
                    if (attempt > 20)
                        throw;

                    Console.WriteLine("ProvisionedThroughputExceededException thrown, retrying...");

                    // need to sleep and try again
                    Thread.Sleep(attempt * 100);
                }
            }
        }

    }
}
