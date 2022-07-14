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

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.kinesis.KinesisPlugin;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.commons.math3.stat.inference.TestUtils;
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

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.io.File.createTempFile;

public class AmazonKinesisUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonKinesisUtils.class);
    protected static final String GOOGLE_SHEETS = "gsheets";
    protected static final String METADATA_SHEET_ID = "metadata_sheet_id";
    static QueryRunner queryRunner;

    private AmazonKinesisUtils() {
    }

    public static QueryRunner getQueryRunner() throws IOException {
        try {

            KinesisPlugin kinesisPlugin = new KinesisPlugin();
            queryRunner = LocalQueryRunner.builder(createSession()).build();

            queryRunner.installPlugin(kinesisPlugin);

            queryRunner.createCatalog(GOOGLE_SHEETS, GOOGLE_SHEETS,
                    getConfigProperties(getCredentialsAsString()));

            System.out.println(" STEP 1: queryRunner object created");

        } catch (Exception exception) {
            System.out.println("Util=GoogleSheetsUtils|Method=getQueryRunner|message=Error occurred " + exception.getMessage());
            throw new IOException(exception);
        }

        return queryRunner;
    }

    public static String getCredentialsAsString() {
        AWSSecretsManager secretsManager = AWSSecretsManagerClientBuilder.defaultClient();
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest();
        getSecretValueRequest.setSecretId( getSecretManagerIdForGsheetQuery ());
        GetSecretValueResult response = secretsManager.getSecretValue(getSecretValueRequest);
        LOGGER.info("response: {}", response);
        System.out.println(" STEP 2: getCredentialsAsString succeded: " + response.getSecretString());
        return response.getSecretString();
    }


    public static Path getCredentialsPath(String encodedCredentials)
            throws Exception {

        File tempFile = createTempFile(System.getProperty("java.io.tmpdir"),
                "credentials-" + System.currentTimeMillis() + ".json");
        tempFile.deleteOnExit();
        Files.write(tempFile.toPath(), encodedCredentials.getBytes());
        return tempFile.toPath();
    }

    static Session createSession() {
        return testSessionBuilder()
                .setCatalog(GOOGLE_SHEETS)
                .setSchema("default")
                .build();
    }

    public static String getMetadataSheetId(String jsonString )
    {
        try {
            JSONParser jsonParser = new JSONParser();
            JSONObject object = (JSONObject) jsonParser.parse( jsonString ) ;
            System.out.println( " STEP 04 metadata sheet id: " + object.get( "metadata_sheet_id"));
            return object.get( METADATA_SHEET_ID ).toString();

        } catch (ParseException e) {
            LOGGER.info( e.toString() );
        }
        return null;
    }

    public static Map<String, String> getConfigProperties(String json) throws Exception {
        System.out.println(" STEP 0.1 json: Secret String:  " + json);
        Path credentialsFile = getCredentialsPath(json);
        System.out.println(" STEP 0.2 : credentialsFile path: " + credentialsFile.toString());

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("credentials-path", credentialsFile.toString())
                .put("metadata-sheet-id", getMetadataSheetId (json ))
                .put("sheets-data-max-cache-size", "20000")
                .put("sheets-data-expire-after-write", "1m")
                .build();

        return properties;
    }

    public static String getSecretManagerIdForGsheetQuery() {
        return getEnvVar(AmazonKinesisConstants.ENV_GOOGLE_SHEET_QUERY_CREDS_SM_ID);
    }

    public static String getEnvVar(String envVar) {
        String envVariable = System.getenv(envVar);
        if (envVariable == null || envVariable.length() == 0) {
            System.out.println("Util=GoogleSheetsUtils|Method=getEnvVar|message=Environment variable name " + envVar + " not found");
            throw new IllegalArgumentException("Lambda Environment Variable " + envVar + " has not been populated! ");
        }
        System.out.println("Util=GoogleSheetsUtils|Method=getEnvVar|message=Value of environment variable '" + envVar + "' is " + envVariable);
        return envVariable;
    }

    /**
     * Gets the project name that exists within Google Cloud Platform that contains the datasets that we wish to query.
     * The Lambda environment variables are first inspected and if it does not exist, then we take it from the catalog
     * name in the request.
     *
     * @param catalogNameFromRequest The Catalog Name from the request that is passed in from the Athena Connector framework.
     * @return The project name.
     */
    public static String getProjectName(String catalogNameFromRequest) {
        if (System.getenv(AmazonKinesisConstants.GCP_PROJECT_ID) != null) {
            return System.getenv(AmazonKinesisConstants.GCP_PROJECT_ID);
        }
        return catalogNameFromRequest;
    }

    /**
     * Gets the project name that exists within Google Cloud Platform that contains the datasets that we wish to query.
     * The Lambda environment variables are first inspected and if it does not exist, then we take it from the catalog
     * name in the request.
     *
     * @param request The {@link MetadataRequest} from the request that is passed in from the Athena Connector framework.
     * @return The project name.
     */

}
