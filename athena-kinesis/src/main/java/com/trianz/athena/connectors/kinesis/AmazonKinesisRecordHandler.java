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
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This record handler is an example of how you can implement a lambda that calls googlesheets and pulls data.
 * This Lambda requires that your BigQuery table is small enough so that a table scan can be completed
 * within 5-10 mins or this lambda will time out and it will fail.
 */
public class AmazonKinesisRecordHandler
        extends RecordHandler {
    private static final Logger logger = LoggerFactory.getLogger(AmazonKinesisRecordHandler.class);


    private  QueryRunner queryRunner;

    AmazonKinesisRecordHandler()
            throws IOException {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(),
                AmazonKinesisUtils.getQueryRunner()
        );
        System.out.println( " STEP 2.0  GoogleSheetsRecordHandler constructor() ");
    }

    @VisibleForTesting
    public AmazonKinesisRecordHandler(AmazonS3 amazonS3,
                                      AWSSecretsManager secretsManager,
                                      AmazonAthena athena,
                                      QueryRunner runner) {
        super(amazonS3, secretsManager, athena, AmazonKinesisConstants.SOURCE_TYPE);
        this.queryRunner = runner;
        System.out.println( " STEP 2.0  GoogleSheetsRecordHandler constructor() ");
    }

    @Override
    public void readWithConstraint(BlockSpiller spiller,
                                   ReadRecordsRequest recordsRequest,
                                   QueryStatusChecker queryStatusChecker) {

        System.out.println( " STEP 2.1  readWithConstraint");

        List<String> parameterValues = new ArrayList<>();
        String sqlToExecute = "";
        try {
            final String projectName = AmazonKinesisUtils.getProjectName(recordsRequest.getCatalogName());
            final String datasetName = recordsRequest.getTableName().getSchemaName();
            final String tableName =  recordsRequest.getTableName().getTableName();

            System.out.println( "STEP  2.1.1 : projectName : " + projectName);
            System.out.println( "STEP  2.1.2: datasetName : " + datasetName);
            System.out.println( "STEP  2.1.3: datasetId  : " + tableName );
            System.out.println( "STEP  2.1.3: recordset getSchema() : " + recordsRequest.getSchema().toString() );
            System.out.println( "STEP  2.1.3: recordset getConstraints() : " + recordsRequest.getConstraints().toString() );
            System.out.println( "STEP  2.1.3: recordset getSplit() : " + recordsRequest.getSplit().toString() );


            sqlToExecute = AmazonKinesisSqlUtils.buildSqlFromSplit(
                    new TableName(datasetName, tableName),
                    recordsRequest.getSchema(),
                    recordsRequest.getConstraints(),
                    recordsRequest.getSplit(),
                    parameterValues);

            System.out.println( "STEP 2.1.4: " + sqlToExecute);

            System.out.println( "STEP 2.1.5: " + parameterValues.toString());


        }
        catch (RuntimeException e) {
            logger.error("Error: ", e);
            e.printStackTrace();
        }
        MaterializedResult result = queryRunner.execute( sqlToExecute );
        outputResults(spiller, recordsRequest , result);
    }
    /**
     * Iterates through all the results that comes back from BigQuery and saves the result to be read by the Athena Connector.
     *
     * @param spiller        The {@link BlockSpiller} provided when readWithConstraints() is called.
     * @param recordsRequest The {@link ReadRecordsRequest} provided when readWithConstraints() is called.
     * @param result         The {@link TableResult} provided by {@link BigQuery} client after a query has completed executing.
     */

    private void outputResults(BlockSpiller spiller, ReadRecordsRequest recordsRequest,
                               MaterializedResult result)
    {
        System.out.println( " STEP 2.2  outputResults");

        List<Field> list = recordsRequest.getSchema().getFields();

        System.out.println( " STEP 2.2.0  schema() " + list.toString() );

        for (MaterializedRow row : result.getMaterializedRows()) {
            spiller.writeRows((Block block, int rowNum) -> {
                boolean isMatched = true;
                int i = 0;
                for (int j = 0; j < row.getFieldCount(); j++) {
                    Field field = null;
                    Object val = row.getField( j ) == null ? "" : row.getField( j ).toString();

                    if (i < list.size()) {
                        field = list.get(i);
                    }
                    ++i;
                    if (i == list.size()) i = 0;

                    isMatched &= block.offerValue(field.getName(), rowNum, val );
                    if (!isMatched) {
                        return 0;
                    }
                }
                return 1;
            });
        }
    }
}