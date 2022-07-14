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

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AmazonKinesisCompositeHandler {
        //extends CompositeHandler {
    private static final Logger logger = LoggerFactory.getLogger(AmazonKinesisCompositeHandler.class);

    /*public AmazonKinesisCompositeHandler()
            throws IOException {
        //super(new AmazonKinesisMetadataHandler(), new AmazonKinesisRecordHandler());
        logger.info("Inside GoogleSheetCompositeHandler()");
    }*/
}
