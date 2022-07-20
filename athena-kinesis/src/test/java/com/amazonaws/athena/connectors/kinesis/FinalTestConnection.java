/*-
 * #%L
 * athena-kinesis
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.kinesis;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.*;
import com.google.common.collect.ImmutableMap;
import com.trianz.athena.connectors.kinesis.util.EmbeddedKinesisStream;
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.kinesis.KinesisPlugin;
import io.trino.plugin.kinesis.util.TestUtils;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;

public class FinalTestConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(FinalTestConnection.class);
    protected static final String KINESIS_SCHEMA = "default";
    static String streamName = "firststream";
    private EmbeddedKinesisStream embeddedKinesisStream;
    static QueryRunner queryRunner;


    public static void main(String[] args) throws Exception {
        FinalTestConnection finalTestConnection = new FinalTestConnection();
        String fullKey = getSecret();
        String accessKey = fullKey.substring(14, 34);
        String secretKey = fullKey.substring(49, 89);
        System.out.println("connection started...");
        finalTestConnection.start(accessKey, secretKey);
        System.out.println("connection successfully....");
        finalTestConnection.spinUp(streamName);
        //QueryRunner runner = getQueryRunner();
        //System.out.println("Spin started...");
        //finalTestConnection.spinUp(accessKey,secretKey);
        //System.out.println("SpinUp ended...");
        try {
            finalTestConnection.runQuery(streamName);
        }catch(Exception e){
            System.out.println(e.getMessage());
        }


    }

    public static QueryRunner getQueryRunner() throws IOException {
        try {
            KinesisPlugin sheetsPlugin = new KinesisPlugin();
            queryRunner = LocalQueryRunner.builder(SESSION).build();

            queryRunner.installPlugin(sheetsPlugin);

            String path = "C:\\Users\\manoj.k\\project_folder\\aws-athena-query-federation\\athena-kinesis\\src\\test\\resources\\tableDescriptions\\CustomTable.json";
            //queryRunner.createCatalog(KINESIS_SCHEMA, KINESIS_SCHEMA, start());
            System.out.println("inside GetQuery Runner ..");
            Map<String, String> kinesisConfig = ImmutableMap.of(
                    "kinesis.default-schema", "default",
                    "kinesis.access-key", "AKIAWIRSZAC4N55LLP5F",
                    "kinesis.secret-key", "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",
                    "kinesis.table-description-location", path);

            queryRunner.createCatalog("kinesis", "kinesis", kinesisConfig);

            System.out.println(" STEP 1: queryRunner object created");

        } catch (Exception exception) {
            System.out.println("Util=GoogleSheetsUtils|Method=getQueryRunner|message=Error occurred " + exception.getMessage());
            throw new IOException(exception);
        }

        return queryRunner;
    }


    private void start(String accessKey, String secretKey) {
        embeddedKinesisStream = new EmbeddedKinesisStream((accessKey), (secretKey));
    }

    /*public void spinUp(String accessKey, String secretKey)
            throws Exception
    {
        //streamName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");


        //embeddedKinesisStream.createStream(2, streamName);
        queryRunner = LocalQueryRunner.builder(createSession()).build();
        //this.queryRunner = new StandaloneQueryRunner(SESSION);
        Path tempDir = Files.createTempDirectory("tempdir");
        File baseFile = new File("src/test/resources/tableDescriptions/EmptyTable.json");
        File file = new File(tempDir.toAbsolutePath().toString() + "/" + streamName + ".json");

        try (Stream<String> lines = Files.lines(baseFile.toPath())) {
            List<String> replaced = lines
                    .map(line -> line.replaceAll("TABLE_NAME", streamName))
                    .map(line -> line.replaceAll("STREAM_NAME", streamName))
                    .collect(Collectors.toList());
            Files.write(file.toPath(), replaced);
        }
        TestUtils.installKinesisPlugin(queryRunner, tempDir.toAbsolutePath().toString(),
                TestUtils.noneToBlank(accessKey), TestUtils.noneToBlank(secretKey));
    }*/

    public static String getSecret() {

        String secretName = "secretKey-Kinesis";
        String endpoint = "secretsmanager.us-east-1.amazonaws.com";
        String region = "us-east-1";

        AwsClientBuilder.EndpointConfiguration config = new AwsClientBuilder.EndpointConfiguration(endpoint, region);
        AWSSecretsManagerClientBuilder clientBuilder = AWSSecretsManagerClientBuilder.standard();
        clientBuilder.setEndpointConfiguration(config);
        AWSSecretsManager client = clientBuilder.build();

        String secret = null;
        ByteBuffer binarySecretData;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                .withSecretId(secretName).withVersionStage("AWSCURRENT");
        GetSecretValueResult getSecretValueResult = null;
        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);

        } catch (ResourceNotFoundException e) {
            System.out.println("The requested secret " + secretName + " was not found");
        } catch (InvalidRequestException e) {
            System.out.println("The request was invalid due to: " + e.getMessage());
        } catch (InvalidParameterException e) {
            System.out.println("The request had invalid params: " + e.getMessage());
        }

        if (getSecretValueResult == null) {
            return null;
        }

        // Depending on whether the secret was a string or binary, one of these fields will be populated
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
            return secret;
        } else {
            binarySecretData = getSecretValueResult.getSecretBinary();
            System.out.println(binarySecretData.toString());
        }
        return null;

    }

    public static final Session SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(Identity.ofUser("user"))
            .setSource("source")
            .setCatalog("kinesis1")
            .setSchema("default")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .setQueryId(new QueryId("dummy"))
            .build();
    /*static Session createSession() {
        return testSessionBuilder()
                .setCatalog("kinesis")
                .setSchema("default")
                .build();
    }*/
    public void runQuery(String streamName) throws IOException {
        System.out.println("inside Run Query....");
        //System.out.println("Values..."+getQueryRunner());
        //queryRunner = getQueryRunner();
        System.out.println("after queryRunner initialezed ...");
        //createJsonMessages( streamName, 1 , false);
        MaterializedResult result = queryRunner.execute("SELECT COUNT(1) FROM " + streamName);
        System.out.println("results "+result);
        System.out.println( "Total Records/messages inserted into Kinesis stream" );
        traverseRecords( result);

        String SQL = "select  * from " + streamName;
        result = queryRunner.execute( SQL );
        System.out.println( SQL );
        traverseRecords( result);

        SQL = "select  *  from " + streamName + " where name='AppFlow'";
        result = queryRunner.execute( SQL );
        System.out.println( SQL );
        traverseRecords( result);

        SQL = "select  name from " + streamName + " where name like 'A%'";
        result = queryRunner.execute( SQL );
        System.out.println( SQL );
        traverseRecords( result);
    }

    public void traverseRecords(MaterializedResult result)
    {
        for( int i = 0; i < result.getRowCount(); i++){
            MaterializedRow row = result.getMaterializedRows().get( i );
            if( row.getFieldCount() == 0 ) System.out.println( " NO RECORDS AVAILABLE ");
            for (int j = 0; j < row.getFieldCount(); j++){
                System.out.println( row.getField( j ));
            }
        }
    }

    //stream data reading....
    public void spinUp( String streamName)
            throws Exception
    {
        System.out.println( "Creating KINESIS stream : " + streamName);
        //embeddedKinesisStream.createStream(2, streamName);
        System.out.println( "Creating KINESIS stream Done : " + streamName);
        this.queryRunner = LocalQueryRunner.builder(SESSION).build();
        System.out.println("query runner available ....");
        Path tempDir = Files.createTempDirectory("tempdir");

        File baseFile = new File("C:\\Users\\manoj.k\\project_folder\\aws-athena-query-federation\\athena-kinesis\\src\\test\\resources\\tableDescriptions\\CustomTable.json");
        File file = new File(tempDir.toAbsolutePath().toString() + "/" + streamName + ".json");
        System.out.println("query1 runner available ....");
        try (Stream<String> lines = Files.lines(baseFile.toPath())) {
            List<String> replaced = lines
                    .map(line -> line.replaceAll("TABLE_NAME", streamName))
                    .map(line -> line.replaceAll("STREAM_NAME", streamName))
                    .collect(Collectors.toList());
            Files.write(file.toPath(), replaced);
            System.out.println("query2 runner available ....");
        }

        TestUtils.installKinesisPlugin(queryRunner, tempDir.toAbsolutePath().toString(),
                TestUtils.noneToBlank("AKIAWIRSZAC4JFB22YG5"),
                TestUtils.noneToBlank("c9BjDlTacT53hDEtphTOS+lMaLSUXrcu/bUwGBGO"));

        System.out.println( "QueryRunner created Successfully" + queryRunner.toString());

    }
}
