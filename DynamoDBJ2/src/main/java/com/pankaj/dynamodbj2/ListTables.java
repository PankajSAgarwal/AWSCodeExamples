package com.pankaj.dynamodbj2;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;

import java.util.List;

public class ListTables {
    public static void main(String[] args) {
        System.out.println("Listing your Amazon DynamoDB tables:\n");
        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build();
        listAllTables(ddb);
        ddb.close();
    }

    private static void listAllTables(DynamoDbClient ddb) {
        boolean moreTables = true;
        String lastName = null;
        while(moreTables) {
            try {
                ListTablesResponse response;
                if(lastName == null) {
                    response = ddb.listTables(ListTablesRequest.builder().build());
                }else {
                    response = ddb.listTables(ListTablesRequest.builder()
                            .exclusiveStartTableName(lastName)
                            .build());
                }

                List<String> tableNames = response.tableNames();
                if(tableNames.size() > 0) {
                    tableNames.forEach(tableName -> System.out.format("* %s\n", tableName));
                }else {
                    System.out.println("No tables found!");
                    System.exit(0);
                }

                lastName = response.lastEvaluatedTableName();
                if(lastName == null) moreTables = false;
            } catch (DynamoDbException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
        System.out.println("\nDone!");
    }
}
