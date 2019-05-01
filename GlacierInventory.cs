using Amazon.Glacier;
using Amazon.Glacier.Model;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace GlacierTools
{
    class GlacierInventory
    {
        static string topicArn;
        static string queueUrl;
        static string queueArn;
        static AmazonSimpleNotificationServiceClient snsClient;
        static AmazonSQSClient sqsClient;
        const string SQS_POLICY =
            "{" +
            "    \"Version\" : \"2012-10-17\"," +
            "    \"Statement\" : [" +
            "        {" +
            "            \"Sid\" : \"glacier-sqs-sendmessage\"," +
            "            \"Effect\" : \"Allow\"," +
            "            \"Principal\" : \"*\"," + // {\"AWS\" : \"{AccountArn}\" }," +
            "            \"Action\"    : \"sqs:SendMessage\"," +
            "            \"Resource\"  : \"{QuernArn}\"," +
            "            \"Condition\" : {" +
            "                \"ArnLike\" : {" +
            "                    \"aws:SourceArn\" : \"{TopicArn}\"" +
            "                }" +
            "            }" +
            "        }" +
            "    ]" +
            "}";

        static string awsAccount = "";
        static string accountArn = "";

        public static void GetInventory(string vaultName, string jobId, string outputPath, Amazon.RegionEndpoint awsRegion)
        {
            AmazonGlacierClient client;
            try
            {
                // Get AWS account info (needed to configure the SNS queue)
                using (var iamClient = new AmazonSecurityTokenServiceClient(awsRegion))
                {
                    var identityResponse = iamClient.GetCallerIdentity(new GetCallerIdentityRequest());
                    awsAccount = identityResponse.Account;
                    accountArn = identityResponse.Arn;
                }

                Console.WriteLine("accountArn: {accountArn}");

                using (client = new AmazonGlacierClient(awsRegion))
                {
                    if (string.IsNullOrEmpty(jobId))
                    {
                        Console.WriteLine("Setup SNS topic and SQS queue.");
                        SetupTopicAndQueue(awsRegion);
                        Console.WriteLine("To continue, press Enter");
                        Console.ReadKey();

                        Console.WriteLine("Retrieve Inventory List");
                        jobId = GetVaultInventoryJobId(vaultName, client);

                        // Check queue for a message and if job completed successfully, download inventory.
                        ProcessQueue(vaultName, jobId, client, outputPath);
                    }
                    else
                    {
                        Console.WriteLine("Downloading job output");
                        DownloadOutput(vaultName, jobId, client, outputPath); // Save job output to the specified file location.
                    }
                }
                Console.WriteLine("Operations successful.");
                Console.WriteLine("To continue, press Enter");
                Console.ReadKey();
            }
            catch (AmazonGlacierException e) { Console.WriteLine(e.Message); }
            catch (AmazonServiceException e) { Console.WriteLine(e.Message); }
            catch (Exception e) { Console.WriteLine(e.Message); }
            finally
            {
                // Delete SNS topic and SQS queue.
                if (!string.IsNullOrEmpty(topicArn))
                    snsClient.DeleteTopic(new DeleteTopicRequest() { TopicArn = topicArn });

                if (!string.IsNullOrEmpty(queueUrl))
                    sqsClient.DeleteQueue(new DeleteQueueRequest() { QueueUrl = queueUrl });
            }
        }

        static void SetupTopicAndQueue(Amazon.RegionEndpoint awsRegion)
        {
            long ticks = DateTime.Now.Ticks;

            // Setup SNS topic.
            snsClient = new AmazonSimpleNotificationServiceClient(awsRegion);
            sqsClient = new AmazonSQSClient(awsRegion);

            topicArn = snsClient.CreateTopic(new CreateTopicRequest { Name = "GlacierInventory-" + ticks }).TopicArn;
            Console.WriteLine($"topicArn: {topicArn}");

            CreateQueueRequest createQueueRequest = new CreateQueueRequest();
            createQueueRequest.QueueName = "GlacierInventory-" + ticks;
            CreateQueueResponse createQueueResponse = sqsClient.CreateQueue(createQueueRequest);
            queueUrl = createQueueResponse.QueueUrl;
            Console.WriteLine($"QueueURL: {queueUrl}");

            GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest();
            getQueueAttributesRequest.AttributeNames = new List<string> { "QueueArn" };
            getQueueAttributesRequest.QueueUrl = queueUrl;
            GetQueueAttributesResponse response = sqsClient.GetQueueAttributes(getQueueAttributesRequest);
            queueArn = response.QueueARN;
            Console.WriteLine($"QueueArn: {queueArn}");

            // Setup the Amazon SNS topic to publish to the SQS queue.
            snsClient.Subscribe(new SubscribeRequest()
            {
                Protocol = "sqs",
                Endpoint = queueArn,
                TopicArn = topicArn
            });

            // Add the policy to the queue so SNS can send messages to the queue.
            var policy = SQS_POLICY.Replace("{TopicArn}", topicArn).Replace("{QuernArn}", queueArn).Replace("{AccountArn}", accountArn);

            sqsClient.SetQueueAttributes(new SetQueueAttributesRequest()
            {
                QueueUrl = queueUrl,
                Attributes = new Dictionary<string, string>
                {
                    { QueueAttributeName.Policy, policy }
                }
            });
        }

        static string GetVaultInventoryJobId(string vaultName, AmazonGlacierClient client)
        {
            // Initiate job.
            InitiateJobRequest initJobRequest = new InitiateJobRequest()
            {
                VaultName = vaultName,
                JobParameters = new JobParameters()
                {
                    Type = "inventory-retrieval",
                    Description = DateTime.Now.ToString() + ": This job is to download a vault inventory.",
                    SNSTopic = topicArn,
                }
            };

            InitiateJobResponse initJobResponse = client.InitiateJob(initJobRequest);
            string jobId = initJobResponse.JobId;

            return jobId;
        }

        private static void ProcessQueue(string vaultName, string jobId, AmazonGlacierClient client, string outputPath)
        {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest() { QueueUrl = queueUrl, MaxNumberOfMessages = 1 };
            bool jobDone = false;
            while (!jobDone)
            {
                Console.WriteLine("{0} Poll SQS queue", DateTime.Now.ToString("HH:mm:ss"));
                ReceiveMessageResponse receiveMessageResponse = sqsClient.ReceiveMessage(receiveMessageRequest);
                if (receiveMessageResponse.Messages.Count == 0)
                {
                    Thread.Sleep(60 * 1000);
                    continue;
                }

                Console.WriteLine("Got message");
                Message message = receiveMessageResponse.Messages[0];
                Dictionary<string, string> outerLayer = JsonConvert.DeserializeObject<Dictionary<string, string>>(message.Body);
                Dictionary<string, object> fields = JsonConvert.DeserializeObject<Dictionary<string, object>>(outerLayer["Message"]);
                string statusCode = fields["StatusCode"] as string;

                if (string.Equals(statusCode, GlacierUtils.JOB_STATUS_SUCCEEDED, StringComparison.InvariantCultureIgnoreCase))
                {
                    Console.WriteLine("Downloading job output");
                    DownloadOutput(vaultName, jobId, client, outputPath); // Save job output to the specified file location.
                }
                else if (string.Equals(statusCode, GlacierUtils.JOB_STATUS_FAILED, StringComparison.InvariantCultureIgnoreCase))
                    Console.WriteLine("Job failed... cannot download the inventory.");

                jobDone = true;
                sqsClient.DeleteMessage(new DeleteMessageRequest() { QueueUrl = queueUrl, ReceiptHandle = message.ReceiptHandle });
            }
        }

        private static void DownloadOutput(string vaultName, string jobId, AmazonGlacierClient client, string outputPath)
        {
            GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
            {
                JobId = jobId,
                VaultName = vaultName
            };

            GetJobOutputResponse getJobOutputResponse = client.GetJobOutput(getJobOutputRequest);
            using (Stream webStream = getJobOutputResponse.Body)
            {
                using (Stream fileToSave = File.Open(outputPath, FileMode.Create))
                {
                    CopyStream(webStream, fileToSave);
                }
            }
        }

        public static void CopyStream(Stream input, Stream output)
        {
            byte[] buffer = new byte[65536];
            int length;
            while ((length = input.Read(buffer, 0, buffer.Length)) > 0)
            {
                output.Write(buffer, 0, length);
            }
        }
    }
}
