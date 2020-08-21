using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Newtonsoft.Json;
using TinyCsvParser;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace dotnet_sqs_lambda
{
    public class Function
    {
        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {

        }


        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
        {
            foreach (var message in evnt.Records)
            {
                await ProcessMessageAsync(message, context);
            }
        }

        private async Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
        {
            context.Logger.LogLine($"Processed message {message.Body}");
            var messageBody = JsonConvert.DeserializeObject<S3EventNotification>(message.Body);

            var products = await ReadS3Message(messageBody.Records[0], context);

            await SaveProductsAsync(products);

        }
        
        private async Task SaveProductsAsync(List<Product> products)
        {
        


            // TODO: Do interesting work based on the new message
            await Task.CompletedTask;
        }

        private async Task<List<Product>> ReadS3Message(S3EventNotification.S3EventNotificationRecord notification, 
            ILambdaContext context)
        {
            var result = new List<Product>();
            try
            {

                var client = new AmazonS3Client(RegionEndpoint.USEast1);
                var request = new GetObjectRequest
                {
                    BucketName = notification.S3.Bucket.Name,
                    Key = notification.S3.Object.Key
                };

                using (GetObjectResponse response = await client.GetObjectAsync(request))
                using (var responseStream = response.ResponseStream)
                using (var reader = new StreamReader(responseStream))
                {
                    var responseBody = reader.ReadToEnd();

                    result = ParseCSV(responseBody);                    
                }

                var resultString = JsonConvert.SerializeObject(result);
                context.Logger.LogLine(resultString);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered ***. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }

            return result;

        }
        private List<Product> ParseCSV(string csvToParse)
        {
            var csvParserOptions = new CsvParserOptions(true, ',');
            var csvMapper = new CsvProductMapping();
            var csvParser = new CsvParser<Product>(csvParserOptions, csvMapper);

            var result = csvParser.ReadFromString(new CsvReaderOptions(new[] { Environment.NewLine }), csvToParse).ToList();

            return result.Where(r => r.IsValid).Select(r => r.Result).ToList();

        }
    }
}

