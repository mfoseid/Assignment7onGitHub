using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Assignment8Triggers
{
    public class Function
    {
        [Serializable]
        public class Item
        {
            public string itemId; //primary key 
            public string description;
            public int rating;
            public string type;
            public string company;
            public string lastInstanceOf;
        }
        [Serializable]
        public class RatingItem
        {
            public string type;
            public int count;
            public double averageRating;
        }
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        public async Task<List<Item>> FunctionHandler(DynamoDBEvent input, ILambdaContext context)
        {
            Table table = Table.LoadTable(client, "RatingsByType");
            List<Item> items = new List<Item>();
            List<DynamoDBEvent.DynamodbStreamRecord> records = (List<DynamoDBEvent.DynamodbStreamRecord>)input.Records;
            if (records.Count > 0)
            {
                DynamoDBEvent.DynamodbStreamRecord record = records[0];
                if (record.EventName.Equals("INSERT"))
                {
                    Document myDoc = Document.FromAttributeMap(record.Dynamodb.NewImage);
                    Item myItem = JsonConvert.DeserializeObject<Item>(myDoc.ToJson());
                    Document ratingsDoc = await table.GetItemAsync(myItem.type);
                    RatingItem ratingsItem = new RatingItem();
                    if (ratingsDoc != null)
                    {
                       ratingsItem = JsonConvert.DeserializeObject<RatingItem>(ratingsDoc.ToJson());
                    }

                    /*if (ratingsItem.averageRating == 0)
                    {
                        ratingsItem.averageRating = myItem.rating;
                    }*/
                    if(ratingsItem.count == 0) 
                    {
                        ratingsItem.count = 1;
                    }
                    ratingsItem.averageRating = (ratingsItem.averageRating + myItem.rating) / ratingsItem.count;

                    var request = new UpdateItemRequest
                    {
                        TableName = "RatingsByType",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            { "type" , new AttributeValue { S = myItem.type} }
                        },
                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        {
                            { "count", new AttributeValueUpdate { Action = "ADD", Value = new AttributeValue { N = "1" } } },
                            
                            { "averageRatings", new AttributeValueUpdate { Action = "PUT", Value = new AttributeValue { N = ratingsItem.averageRating.ToString() } } },
                        },
                    };
                    await client.UpdateItemAsync(request);
                }//end of if 
            }
            return items;
        }
    }
}
