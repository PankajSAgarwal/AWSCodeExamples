package com.pankaj.dynamodbj2;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

public class UpdateTable {
    public static void main(String[] args) {
        final String usage = """
                Usage:
                    <tableName> <readCapacity> <writeCapacity>
                Where:
                    tableName - The Amazon DynamoDB table to get information (for example, Music3).
                    readCapacity - The new read capacity of the table
                    writeCapacity - The new write capacity of the table
                    
                Example: UpdateTable Music3 16 10
                """;

        if (args.length != 3) {
            System.out.println(usage);
            System.exit(1);
        }

        String tableName = args[0];
        long readCapacity = Long.parseLong(args[1]);
        long writeCapacity = Long.parseLong(args[2]);

        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build();
        updateDynamoDBTable(ddb, tableName, readCapacity, writeCapacity);
        ddb.close();
    }

    private static void updateDynamoDBTable(DynamoDbClient ddb, String tableName, long readCapacity, long writeCapacity) {
        System.out.format("Updating %s with new provisioned throughput value\n", tableName);
        System.out.format("Read Capacity: %d\n", readCapacity);
        System.out.format("Write Capacity: %d\n", writeCapacity);

        ProvisionedThroughput tableThroughPut = ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacity)
                .writeCapacityUnits(writeCapacity)
                .build();

        try {

            ddb.updateTable(UpdateTableRequest.builder()
                    .provisionedThroughput(tableThroughPut)
                    .tableName(tableName)
                    .build());

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        System.out.println("Done!");

    }
}
