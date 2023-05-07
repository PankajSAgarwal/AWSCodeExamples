package com.pankaj.dynamodbj2;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

public class CreateTableCompositeKey {
    public static void main(String[] args) {
        final String usage = """
                Usage:
                    <tableName> <key>

                Where:
                    tableName - The Amazon DynamoDB table to create (for example, Music3).

                    key - The key for the Amazon DynamoDB table (for example, Artist).
                """;
        if(args.length != 1){
            System.out.println(usage);
            System.exit(1);
        }
        String tableName = args[0];
        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build();

        System.out.format("Creating Amazon DynamoDB table %s\n with a composite primary key:\n", tableName);
        System.out.format("* Language - partition key\n");
        System.out.format("* Greeting - sort key\n");
        String tableId = createTableComKey(ddb, tableName);
        System.out.println("The Amazon DynamoDB table Id value is " + tableId);
        ddb.close();


    }

    private static String createTableComKey(DynamoDbClient ddb, String tableName) {
        DynamoDbWaiter waiter = ddb.waiter();
        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("Language")
                        .attributeType(ScalarAttributeType.S)
                        .build(), AttributeDefinition.builder()
                        .attributeName("Greeting")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName("Language")
                        .keyType(KeyType.HASH)
                        .build(), KeySchemaElement.builder()
                        .attributeName("Greeting")
                        .keyType(KeyType.RANGE)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(10L)
                        .writeCapacityUnits(10L)
                        .build())
                .tableName(tableName)
                .build();

        try {
            CreateTableResponse response = ddb.createTable(request);
            WaiterResponse<DescribeTableResponse> waiterResponse = waiter.waitUntilTableExists(DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build());
            waiterResponse.matched().response().ifPresent(System.out::println);
            return response.tableDescription().tableId();
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return " ";
    }
}
