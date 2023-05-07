package com.pankaj.dynamodbj2;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class DeleteTable {
    public static void main(String[] args) {
        final String usage = """
                Usage:
                    <tableName> <key>

                Where:
                    tableName - The Amazon DynamoDB table to delete (for example, Music3).
                *Warnings* - These program will delete the tables that you specify .
                    
                """;
        if(args.length != 1){
            System.out.println("usage = " + usage);
            System.exit(1);
        }

        String tableName = args[0];
        System.out.format("Deleting the Amazon DynamoDB table %s ... \n", tableName);
        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build();
        deleteDynamoDB(ddb, tableName);
        ddb.close();
    }

    private static void deleteDynamoDB(DynamoDbClient ddb, String tableName) {
        try {
            ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
        } catch(DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.out.println(tableName + " was successfully deleted . ");
    }
}
