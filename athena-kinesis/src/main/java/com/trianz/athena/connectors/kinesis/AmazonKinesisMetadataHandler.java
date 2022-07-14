/*-
 * #%L
 * trianz-googlesheets-athena-google
 * %%
 * Copyright (C) 2019 - 2021 Trianz
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.trianz.athena.connectors.kinesis;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.google.common.annotations.VisibleForTesting;
//import com.trianz.afq.license.impl.TrianzAFQLicenceValidator;
import com.trianz.afq.license.impl.TrianzAFQLicenceValidator;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class AmazonKinesisMetadataHandler {
        //extends MetadataHandler {

   /* private static final String DEFAULT_SCHEMA_NAME = "default";
    private static final Logger logger = LoggerFactory.getLogger(AmazonKinesisMetadataHandler.class);

    private static QueryRunner queryRunner;

    AmazonKinesisMetadataHandler()
            throws IOException {
        this( AmazonKinesisUtils.getQueryRunner());
    }

    @VisibleForTesting
    public AmazonKinesisMetadataHandler(QueryRunner runner) {
        super(AmazonKinesisConstants.SOURCE_TYPE);
        queryRunner = runner;
        System.out.println( " STEP 1.0 GoogleSheetsMetadataHandler constructor() ");
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest) {
        try {
            System.out.println( " STEP 1.1 doListSchemaNames ");
            logger.info("doListSchemaNames called with Catalog: {}", listSchemasRequest.getCatalogName());

            final List<String> schemas = new ArrayList<>();

            // TODO: for now, only the default schema
            schemas.add(DEFAULT_SCHEMA_NAME);

            logger.info("Found {} schemas!", schemas.size());
            System.out.println( " STEP 8: " + schemas.size());
            return new ListSchemasResponse(listSchemasRequest.getCatalogName(), schemas);

        } catch (Exception exception) {
            logger.error("Error: ", exception);
            System.err.println("Source=GoogleSheetsMetadataHandler|Method=doListSchemaNames|message=Error occurred "
                    + exception.getMessage());
            exception.printStackTrace();
        }
        return null;
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest) {
        try {
            System.out.println( " STEP 1.2  doListTables");
            logger.info("doListTables called with request {}:{}", listTablesRequest.getCatalogName(),
                    listTablesRequest.getSchemaName());
            System.out.println( " STEP 7: ");
            List<TableName> tableNames = new ArrayList<>();
            MaterializedResult result = queryRunner.execute("show tables");
            for (MaterializedRow row : result.getMaterializedRows()) {
                System.out.println("Table name: " + row.getField(0));
                if (row.getField(0) != null) {
                    tableNames.add(new TableName(DEFAULT_SCHEMA_NAME, row.getField(0).toString().toLowerCase(Locale.ROOT)));
                }
            }

            logger.info("Found {} table(s)!", tableNames.size());

            return new ListTablesResponse(listTablesRequest.getCatalogName(), tableNames, null);
        } catch (Exception e) {
            logger.error("Error:", e);
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest) {
        try {
           String licenceKey = System.getenv().get("licenceKey");

            //String licenceKey = AmazonKinesisUtils.getEnvDetails().get("licenceKey");

            System.out.println( "STEP 1.3.0 licencekey : " + licenceKey );

            System.out.println( " STEP 1.3  doGetTable");
            System.out.println( " STEP 4: "+ AmazonKinesisUtils.getProjectName(getTableRequest));
            System.out.println( " STEP 4: "+ getTableRequest.getTableName());

            logger.info("doGetTable called with request {}:{}", AmazonKinesisUtils.getProjectName(getTableRequest),
                    getTableRequest.getTableName());

            final Schema tableSchema = getSchema(getTableRequest.getTableName().getTableName());

            System.out.println( tableSchema.toString());
            TrianzAFQLicenceValidator validator = new TrianzAFQLicenceValidator();
            validator.validateLicense(licenceKey);
            System.out.println( "Licence Validation Successful");
            return new GetTableResponse(AmazonKinesisUtils.getProjectName(getTableRequest).toLowerCase(),
                    getTableRequest.getTableName(), tableSchema);
        } catch (Exception e) {
            logger.error("Error: ", e);
        }
        return null;
    }

    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) {
        //NoOp since we don't support partitioning at this time.
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) {
        System.out.println( " STEP 1.4  doGetSplits");

        String projectName = AmazonKinesisUtils.getProjectName(request);
        String dataSetName = request.getTableName().getSchemaName();
        String tableName = request.getTableName().getTableName();

        String sql = "select count(*) FROM "  + DEFAULT_SCHEMA_NAME + "." + tableName;
        System.out.println( " STEP 1.4.1 SQL : " + sql );

        MaterializedResult result = queryRunner.execute( sql);
        double numberOfRows = 0;
        if (!result.getMaterializedRows().isEmpty()) {
            MaterializedRow row = result.getMaterializedRows().get(0);
            System.out.println("Table '" + tableName + "' row count : " + row.getField(0));
            if (row.getField(0) != null) {
                numberOfRows = Long.parseLong(row.getField(0).toString());
            }
        }

        System.out.println( " STEP 1.4.2 Number of Rows: " + numberOfRows);

        long pageCount = (long) numberOfRows / 100000;

        System.out.println( " STEP 1.4.3 Page Count : " + pageCount);
        long totalPageCountLimit = (pageCount == 0) ? (long) numberOfRows : pageCount;

        System.out.println( " STEP 1.4.4 total Page Count Limit : " + totalPageCountLimit);
        double limit = (int) Math.ceil(numberOfRows / totalPageCountLimit);

        System.out.println( " STEP 1.4.5 Limit : " + limit);
        Set<Split> splits = new HashSet<>();
        long offSet = 0;

        for ( int i = 1; i <= limit; i++) {
            if (i > 1) {
                offSet = offSet + totalPageCountLimit;
            }
            // Every split must have a unique location if we wish to spill to avoid failures
            SpillLocation spillLocation = makeSpillLocation(request);
            // Create a new split (added to the splits set) that includes the domain and endpoint, and
            // shard information (to be used later by the Record Handler).
            Map<String, String> map = new HashMap<>();
            map.put(Long.toString(totalPageCountLimit), Long.toString(offSet));
            splits.add(new Split(spillLocation, makeEncryptionKey(), map));
        }
        return new GetSplitsResponse(request.getCatalogName(), splits);
    }

    private Schema getSchema(String tableName) {

        System.out.println( " STEP 1.5  getSchema()");
        MaterializedResult result = queryRunner.execute("describe " + tableName);
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        for (MaterializedRow row : result.getMaterializedRows()) {
            System.out.println("Field name: " + row.getField(0));
            if (row.getField(0) != null) {
                schemaBuilder.addField(row.getField(0).toString(), new ArrowType.Utf8());
                System.out.println( " STEP 1.5.1 : Field Name: " + row.getField(0).toString());
                System.out.println( " STEP 1.5.1 Field Type: " + new ArrowType.Utf8());
            }
        }
        return schemaBuilder.build();
    }*/
}