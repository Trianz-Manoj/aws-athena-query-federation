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
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.kinesis.KinesisPlugin;
import io.trino.plugin.kinesis.util.EmbeddedKinesisStream;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class ConnectionTest {
    static String streamName = "firststream";
    private EmbeddedKinesisStream embeddedKinesisStream;
    private QueryRunner queryRunner;

    public static final Session SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(Identity.ofUser("user"))
            .setSource("source")
            .setCatalog("kinesis")
            .setSchema("default")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .setQueryId(new QueryId("dummy"))
            .build();
    static String str = getSecret();
    static String accessKey = str.substring(14, 34);
    static String secretKey = str.substring(49,89);
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionTest.class);
    public static void main(String[] args) throws Exception {
        ConnectionTest test = new ConnectionTest();
        test.getSecret();
        System.out.println(accessKey+","+secretKey);
        test.start();
        test.spinUp(streamName);



    }
    public void start()
    {
        embeddedKinesisStream = new EmbeddedKinesisStream(TestUtils.noneToBlank("accessKey"),
                TestUtils.noneToBlank("secretKey"));
        System.out.println("Connection success....");

    }
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
            /*System.out.println("secret key "+secret);
            String[] secret1 = secret.split(",",0);*/

            return secret;
        } else {
            binarySecretData = getSecretValueResult.getSecretBinary();
            System.out.println(binarySecretData.toString());
        }
        return null;

    }
    public void spinUp( String streamName)
            throws Exception
    {
        try {
            //System.out.println( "Creating KINESIS stream : " + streamName);
            //embeddedKinesisStream.createStream(2, streamName);
            //System.out.println( "Creating KINESIS stream Done : " + streamName);
            //this.queryRunner = new StandaloneQueryRunner(SESSION);
            KinesisPlugin kinesisPlugin = new KinesisPlugin();
            this.queryRunner = LocalQueryRunner.builder(SESSION).build();
            queryRunner.installPlugin(kinesisPlugin);


            Path tempDir = Files.createTempDirectory("tempdir");

            File baseFile = new File("C:\\Trino_project\\trino-master\\plugin\\trino-kinesis\\src\\test\\resources\\tableDescriptions\\CustomTable.json");
            File file = new File(tempDir.toAbsolutePath().toString() + "/" + streamName + ".json");

            try (Stream<String> lines = Files.lines(baseFile.toPath())) {
                List<String> replaced = lines
                        .map(line -> line.replaceAll("TABLE_NAME", streamName))
                        .map(line -> line.replaceAll("STREAM_NAME", streamName))
                        .collect(Collectors.toList());
                Files.write(file.toPath(), replaced);
            }
            Map<String, String> kinesisConfig = ImmutableMap.of(
                    "kinesis.default-schema", "default",
                    "kinesis.access-key", "accessKey",
                    "kinesis.secret-key", "secretKey",
                    "kinesis.table-description-location", tempDir.toAbsolutePath().toString());
            queryRunner.createCatalog("kinesis", "kinesis", kinesisConfig);
            System.out.println(" STEP 1: queryRunner object created");
            MaterializedResult result = queryRunner.execute("SELECT COUNT(1) FROM firststream");
            System.out.println("Total Records/messages inserted into Kinesis stream");
            traverseRecords(result);
        }catch (Exception exception){
            System.out.println("Error occurred " + exception.getMessage());
            throw new IOException(exception);
        }



        System.out.println( "QueryRunner created Successfully" + queryRunner.toString());

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
}
