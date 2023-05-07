package com.pankaj.dynamodbj2;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

public class DescribeTable {
    public static void main(String[] args) {
        final String usage = """
                Usage:
                    <tableName> <key>

                Where:
                    tableName - The Amazon DynamoDB table to get information (for example, Music3).
                """;

        if(args.length != 1){
            System.out.println(usage);
            System.exit(1);
        }

        String tableName = args[0];
        System.out.format("Getting description of DynamoDB table %s\n\n", tableName);

        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build();

        describeDynamoDBTable(ddb, tableName);
        ddb.close();

    }

    private static void describeDynamoDBTable(DynamoDbClient ddb, String tableName) {
        try {
            TableDescription tableInfo = ddb.describeTable(DescribeTableRequest
                    .builder()
                    .tableName(tableName)
                    .build()).table();

            if(tableInfo != null){
                System.out.format("Table name   : %s\n", tableInfo.tableName());
                System.out.format("Table ARN   : %s\n", tableInfo.tableArn());
                System.out.format("Status   : %s\n", tableInfo.tableStatus());
                System.out.format("ItemCount   : %d\n", tableInfo.itemCount());
                System.out.format("Size (bytes)   : %d\n", tableInfo.tableSizeBytes());

                ProvisionedThroughputDescription throughputInfo = tableInfo.provisionedThroughput();

                System.out.println("ThroughPut Info ");
                System.out.format(" Read Capacity : %d\n", throughputInfo.readCapacityUnits());
                System.out.format(" Write Capacity : %d\n", throughputInfo.writeCapacityUnits());

                System.out.println("Attributes");

                tableInfo.attributeDefinitions().forEach(attributeDefinition ->
                        System.out.format(" %s (%s)\n", attributeDefinition.attributeName(), attributeDefinition.attributeType()));

            }
        } catch (DynamoDbException e){
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("\nDone!");
    }
}
