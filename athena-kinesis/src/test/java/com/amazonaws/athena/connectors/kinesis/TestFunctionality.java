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

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.common.collect.ImmutableMap;
import com.trianz.athena.connectors.kinesis.util.EmbeddedKinesisStream;
import io.trino.Session;;
import io.trino.plugin.kinesis.KinesisPlugin;
import io.trino.plugin.kinesis.util.TestUtils;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.io.File.createTempFile;
import static java.util.Locale.ENGLISH;

public class TestFunctionality {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestFunctionality.class);
    protected static final String STREAM_NAME = "firststream";
    protected static final String KINESIS_SCHEMA = "kinesis";
    private EmbeddedKinesisStream embeddedKinesisStream;
    private static GetSecretValue getSecretValue;
    static QueryRunner queryRunner;
    public static void main(String[] args) throws IOException {
        TestFunctionality testFunctionality = new TestFunctionality();
        testFunctionality.getQueryRunner();
    }
    /*private  void start(String accessKey,String secretKey){
        embeddedKinesisStream = new EmbeddedKinesisStream((accessKey),(secretKey));
    }*/

   /* public static void installKinesisPlugin(QueryRunner queryRunner, String tableDescriptionLocation, String accessKey, String secretKey)
    {
        KinesisPlugin kinesisPlugin = new KinesisPlugin();
        queryRunner.installPlugin(kinesisPlugin);

        Map<String, String> kinesisConfig = ImmutableMap.of(
                "kinesis.default-schema", "default",
                "kinesis.access-key", accessKey,
                "kinesis.secret-key", secretKey,
                "kinesis.table-description-location", tableDescriptionLocation);

        queryRunner.createCatalog("kinesis", "kinesis", kinesisConfig);
    }
*/
    public static QueryRunner getQueryRunner() throws IOException {
        try {
            KinesisPlugin sheetsPlugin = new KinesisPlugin();
            queryRunner = LocalQueryRunner.builder(createSession()).build();

            queryRunner.installPlugin(sheetsPlugin);

            queryRunner.createCatalog(KINESIS_SCHEMA, KINESIS_SCHEMA,
                    getConfigProperties(getCredentialsAsString()));

            System.out.println(" STEP 1: queryRunner object created");

        } catch (Exception exception) {
            System.out.println("Util=GoogleSheetsUtils|Method=getQueryRunner|message=Error occurred " + exception.getMessage());
            throw new IOException(exception);
        }

        return queryRunner;
    }



    /*static Session createSession() {
        return Session.builder(new SessionPropertyManager())
                .setIdentity(Identity.ofUser("user"))
                .setSource("source")
                .setSchema("default")
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setQueryId(new QueryId("dummy"))
                .build();
    }*/
    static Session createSession() {
        return testSessionBuilder()
                .setCatalog(KINESIS_SCHEMA)
                .setSchema("default")
                .build();
    }
    public static Map<String, String> getConfigProperties(String json) throws Exception {
        System.out.println(" STEP 0.1 json: Secret String:  " + json);
        Path credentialsFile = getCredentialsPath(json);
        System.out.println(" STEP 0.2 : credentialsFile path: " + credentialsFile.toString());

        Map<String, String> properties = ImmutableMap.of(
                "kinesis.default-schema", "default",
                "kinesis.access-key", "AKIAWIRSZAC4N55LLP5F",
                "kinesis.secret-key", "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",
                "kinesis.table-description-location", "src/test/resources/tableDescriptions/CustomTable.json");
        queryRunner.createCatalog("kinesis", "kinesis", properties);

        return properties;
    }


    public static Path getCredentialsPath(String encodedCredentials)
            throws Exception {

        File tempFile = createTempFile(System.getProperty("java.io.tmpdir"),
                "credentials-" + System.currentTimeMillis() + ".json");
        tempFile.deleteOnExit();
        Files.write(tempFile.toPath(), encodedCredentials.getBytes());
        return tempFile.toPath();
    }
    public static String getCredentialsAsString() {
        AWSSecretsManager secretsManager = AWSSecretsManagerClientBuilder.defaultClient();
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId("secretKey-Kinesis").withVersionStage("AWSCURRENT");
        GetSecretValueResult response = secretsManager.getSecretValue(getSecretValueRequest);
        LOGGER.info("response: {}", response);
        System.out.println(" STEP 2: getCredentialsAsString succeded: " + response.getSecretString());
        return response.getSecretString();
    }
    /*public static String getSecretManagerIdForGsheetQuery() {
        return getEnvVar(AmazonKinesisConstants.ENV_GOOGLE_SHEET_QUERY_CREDS_SM_ID);
    }*/
    /*public static String getEnvVar(String envVar) {
        String envVariable = System.getenv(envVar);
        System.out.println("environment variable "+envVariable);
        if (envVariable == null || envVariable.length() == 0) {
            System.out.println("Util=GoogleSheetsUtils|Method=getEnvVar|message=Environment variable name " + envVar + " not found");
            throw new IllegalArgumentException("Lambda Environment Variable " + envVar + " has not been populated! ");
        }
        System.out.println("Util=GoogleSheetsUtils|Method=getEnvVar|message=Value of environment variable '" + envVar + "' is " + envVariable);
        return envVariable;
    }*/



}

