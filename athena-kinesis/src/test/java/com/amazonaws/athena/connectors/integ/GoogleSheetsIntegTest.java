/*-
 * #%L
 * trianz-googlesheets-athena-sdk
 * %%
 * Copyright (C) 2019 - 2022 Trianz
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
package com.amazonaws.athena.connectors.integ;

import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import com.amazonaws.athena.connector.integ.data.TestConfig;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GoogleSheetsIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsIntegTest.class);
    private final App theApp;
    private final String lambdaFunctionName;

    private static TestConfig testConfig;
    private static String schemaName;
    private static String tableName;
    private static String tableName2;
    private static String partitionTable;
    private static String tableName3;
    private static String lambdaAndSchemaName;

    static {
        testConfig = new TestConfig();
        System.setProperty("aws.region", testConfig.getStringItem("region").get());
        System.setProperty("aws.accessKeyId", testConfig.getStringItem("access_key").get());
        System.setProperty("aws.secretKey", testConfig.getStringItem("secret_key").get());
        schemaName = testConfig.getStringItem("schema_name").get();
        tableName = testConfig.getStringItem("table_name").get();
        tableName2 = testConfig.getStringItem("table_name2").get();
        partitionTable = testConfig.getStringItem("partition_table").get();
        tableName3 = testConfig.getStringItem("table_name3").get();
    }

    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. DynamoDB,
     * Elasticsearch etc...)
     * @return A policy document object.
     */

    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy()
    {
        List<String> statementActionsPolicy = new ArrayList<>();
        statementActionsPolicy.add("*");
        return Optional.of(PolicyDocument.Builder.create()
                .statements(ImmutableList.of(PolicyStatement.Builder.create()
                        .actions(statementActionsPolicy)
                        .resources(ImmutableList.of("*"))
                        .effect(Effect.ALLOW)
                        .build()))
                .build());
    }

    /**
     * Sets the environment variables for the Lambda function.
     */
    @Override
    protected void setConnectorEnvironmentVars(final Map environmentVars)
    {
        // environmentVars.putAll(this.environmentVars);
    }

    @Override
    protected void setUpTableData() {

    }

    /**
     * Sets up connector-specific Cloud Formation resource.
     * @param stack The current CloudFormation stack.
     */
    @Override
    protected void setUpStackData(final Stack stack)
    {
        // No-op.
    }

    @BeforeClass
    @Override
    protected void setUp()
    {
        try {
            super.setUp();
        }
        catch (Exception e) {

            throw e;
        }
    }

    public GoogleSheetsIntegTest()
    {
        logger.warn("Entered constructor");
        theApp = new App();
        lambdaFunctionName = getLambdaFunctionName();
        lambdaAndSchemaName = "\"lambda:"+lambdaFunctionName+"\"."+schemaName+".";
    }



    @Test
    public void sampleTestRunSelect()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableData");
        logger.info("-----------------------------------");
        List firstColValues =  processQuery( "select name, address from \"lambda:"
                +  lambdaFunctionName + "\".\"default\".\"directory\"");
        logger.info("firstColValues: {}", firstColValues);
    }


    @Test
    public void getTableData()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableData");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataSelect(schemaName,tableName,lambdaFunctionName);
        logger.info("firstColValues: {}", firstColValues);
    }

    @Test
    public void getTableCount()
    {
        logger.info("-----------------------------------");
        logger.info("Executing getTableCount");
        logger.info("-----------------------------------");
        List tableCount = fetchDataSelectCountAll(schemaName,tableName,lambdaFunctionName);
        logger.info("table count: {}", tableCount.get(0));
    }

    @Test
    public void useWhereClause()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClause");
        logger.info("-----------------------------------");
        List firstColOfMatchedRows = fetchDataWhereClause(schemaName,tableName,lambdaFunctionName,
                "address","sydney");
        logger.info("firstColOfMatchedRows: {}", firstColOfMatchedRows);
    }
    @Test
    public void useWhereClauseWithLike()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useWhereClauseWithLike");
        logger.info("-----------------------------------");
        List firstColOfMatchedRows = fetchDataWhereClauseLIKE(schemaName,tableName,
                lambdaFunctionName,"name","%M%");
        logger.info("firstColOfMatchedRows: {}", firstColOfMatchedRows);
    }

    @Test
    public void useGroupBy()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupBy");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataGroupBy(schemaName,tableName2,lambdaFunctionName, "productname");
        logger.info("count(name) values: {}", firstColValues);
    }

    @Test
    public void useGroupByHaving()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useGroupByHaving");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataGroupByHavingClause(schemaName,tableName2,lambdaFunctionName,
                "productname","iphone");
        logger.info("count(name) Having 'Confirmed' values : {}", firstColValues);
    }

    @Test
    public void useUnion()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useUnion");
        logger.info("-----------------------------------");
        List firstColValues = fetchDataUnion(schemaName,tableName,tableName,lambdaFunctionName,lambdaFunctionName,
                "name","Vijay");
        logger.info("union where name 'Confirmed' values : {}", firstColValues);
    }


    @Test
    public void useJoin()
    {
        logger.info("-----------------------------------");
        logger.info("Executing useJoin");
        logger.info("-----------------------------------");

        List firstColValues =  processQuery( "select a.name, b.name, b.job from   " +  lambdaFunctionName + ".\"default\".\"directory\"  a ,   "
                + lambdaFunctionName + ".\"default\".\"employee_history\" b where  a.name = b.name and a.name='vijay'");
        logger.info("firstColValues: {}", firstColValues);
    }
}
