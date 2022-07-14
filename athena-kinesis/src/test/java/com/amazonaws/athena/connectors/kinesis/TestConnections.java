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
import com.amazonaws.services.secretsmanager.model.*;
import com.trianz.athena.connectors.kinesis.AmazonKinesisConstants;
import com.trianz.athena.connectors.kinesis.AmazonKinesisUtils;
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Map;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class TestConnections {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestConnections.class);
    public static final Session SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(Identity.ofUser("user"))
            .setSource("source")
            .setCatalog("kinesis")
            .setSchema("default")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .setQueryId(new QueryId("dummy"))
            .build();

    public static void main(String[] args) {
        TestConnections testConnections = new TestConnections();
        System.out.println("connections...");
         testConnections.getCredentialsAsString();
        //testConnections.getSecret();
        System.out.println("Details");

    }

    /*public void start()
    {
        embeddedKinesisStream = new EmbeddedKinesisStream(TestUtils.noneToBlank(""),
                TestUtils.noneToBlank(""));

    }*/

    public static String getCredentialsAsString() {
        AWSSecretsManager secretsManager = AWSSecretsManagerClientBuilder.defaultClient();
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest();
        getSecretValueRequest.setSecretId(getSecretManagerIdForKinesisQuery());
        GetSecretValueResult response = secretsManager.getSecretValue(getSecretValueRequest);
        LOGGER.info("response: {}", response);
        System.out.println(" STEP 2: getCredentialsAsString succeded: " + response.getSecretString());
        return response.getSecretString();
    }

    public static String getSecretManagerIdForKinesisQuery() {
        return getEnvVar(AmazonKinesisConstants.ENV_GOOGLE_SHEET_QUERY_CREDS_SM_ID);
    }

    public static Map<String, String> getEnvDetails() {

        return System.getenv();
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

    public static void getSecret() {

        String secretName = "SecretKeyValue";
        String region = "us-east-1";

        // Create a Secrets Manager client
        AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard()
                .withRegion(region)
                .build();

        // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        // We rethrow the exception by default.

        String secret, decodedBinarySecret;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                .withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;

        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (DecryptionFailureException e) {
            // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            // Deal with the exception here, and/or rethrow at your discretion.
            System.out.println("DecryptionFailureException "+e.getMessage());
        } catch (InternalServiceErrorException e) {
            // An error occurred on the server side.
            // Deal with the exception here, and/or rethrow at your discretion.
            System.out.println("InternalServiceErrorException "+e.getMessage());
        } catch (InvalidParameterException e) {
            // You provided an invalid value for a parameter.
            // Deal with the exception here, and/or rethrow at your discretion.
            System.out.println("InvalidParameterException "+e.getMessage());
        } catch (InvalidRequestException e) {
            // You provided a parameter value that is not valid for the current state of the resource.
            // Deal with the exception here, and/or rethrow at your discretion.
            System.out.println("InvalidRequestException "+e.getMessage());
        } catch (ResourceNotFoundException e) {
            // We can't find the resource that you asked for.
            // Deal with the exception here, and/or rethrow at your discretion.
            System.out.println("ResourceNotFoundException "+e.getMessage());
        }

        // Decrypts secret using the associated KMS key.
        // Depending on whether the secret is a string or binary, one of these fields will be populated.
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
        } else {
            decodedBinarySecret = new String(Base64.getDecoder().decode(getSecretValueResult.getSecretBinary()).array());
        }
    }
}
