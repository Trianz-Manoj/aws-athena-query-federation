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

public class AmazonKinesisConstants {
    /**
     * The source type that is used to aid in logging diagnostic info when raising a support case.
     */
    public static final String SOURCE_TYPE = "googlesheets";

    /**
     * The maximum number of datasets and tables that can be returned from Google BigQuery API calls for metadata.
     */
    public static final long MAX_RESULTS = 10_000;

    /**
     * The Project ID within the Google Cloud Platform where the datasets and tables exist to query.
     */
    public static final String GCP_PROJECT_ID = "gcp_project_id";

    /**
     * The name of the secret within Secrets Manager that contains credentials JSON that provides this Lambda access
     * to call Google BigQuery.
     */
    public static final String ENV_GOOGLE_SHEET_QUERY_CREDS_SM_ID = "kinesis-secret";

//    public static final String ENV_BIG_QUERY_CREDS_SM_ID = "secret_manager_gcp_creds_name";

    private AmazonKinesisConstants() {
    }
}
