// Use AWS SDK v3 with ES module imports
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';

// Set the AWS region
const AWS_REGION = process.env.AWS_REGION || "us-east-1";

// Create DynamoDB client
const client = new DynamoDBClient({ region: AWS_REGION });
const docClient = DynamoDBDocumentClient.from(client);

// Set the DynamoDB table name
const ORDERS_TABLE_NAME = "Orders";

export const handler = async (event) => {
    console.log("Lambda function invoked with event:", JSON.stringify(event));

    // Iterate through each record in the SQS event
    for (const record of event.Records) {
        try {
            const messageBody = JSON.parse(record.body);
            
            // Check if this is an SNS message (has Message property)
            let order;
            if (messageBody.Message) {
                order = JSON.parse(messageBody.Message);
            } else {
                order = messageBody; // Direct SQS message
            }

            console.log("Processing order:", JSON.stringify(order));

            // Prepare the item to be saved to DynamoDB
            const orderItem = {
                orderId: order.orderId,
                userId: order.userId,
                itemName: order.itemName,
                quantity: order.quantity,
                status: order.status,
                timestamp: order.timestamp || new Date().toISOString() // Add timestamp if not present
            };

            const params = {
                TableName: ORDERS_TABLE_NAME,
                Item: orderItem
            };

            // Save the item to DynamoDB using SDK v3 pattern
            console.log("Putting item into DynamoDB:", JSON.stringify(params));
            await docClient.send(new PutCommand(params));
            console.log("Successfully put item into DynamoDB");
        } catch (error) {
            console.error("Error processing SQS message:", error);
            // Re-throw the error to ensure the message is not removed from the queue
            // This is important for DLQ functionality
            throw error;
        }
    }

    console.log("Successfully processed all SQS messages.");
    return { statusCode: 200, body: "Successfully processed messages" };
};