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
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
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
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class KinesisTestStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisTestStream.class);
    public static final Session SESSION = Session.builder(new SessionPropertyManager())
            .setIdentity(Identity.ofUser("user"))
            .setSource("source")
            .setCatalog("kinesis")
            .setSchema("default")
            .setTimeZoneKey(UTC_KEY)
            .setLocale(ENGLISH)
            .setQueryId(new QueryId("dummy"))
            .build();

    private EmbeddedKinesisStream embeddedKinesisStream;
    private static AmazonKinesisClient amazonKinesisClient;
    //private StandaloneQueryRunner queryRunner;
    private static QueryRunner queryRunner;

   /* @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })*/


    public static void main(String[] args) throws Exception {
        String streamName = "firststream";
        KinesisTestStream testMinimalFunctionalityCustomize = new KinesisTestStream();
        testMinimalFunctionalityCustomize.start();
        testMinimalFunctionalityCustomize.spinUp(streamName);
        testMinimalFunctionalityCustomize.getQueryRunner();
        //testMinimalFunctionalityCustomize.runQuery(streamName);
        //List<String> test = testMinimalFunctionalityCustomize.streamNames();
        //System.out.println("information: "+test);
        //testMinimalFunctionalityCustomize.spinUp(streamName);
        //System.out.println(testMinimalFunctionalityCustomize.streamNames());

        //testMinimalFunctionalityCustomize.runQuery(streamName);
        //testMinimalFunctionalityCustomize.stop();
        //System.out.println(testMinimalFunctionalityCustomize.streamNames());
       /* System.out.println("Tearing down  Environment .... Started");
        //testMinimalFunctionalityCustomize.tearDown( streamName);
        System.out.println("Tearing down  Environment .... Done");*/

    }
    public void runQuery(String streamName)
    {

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
    public void start()
    {
        embeddedKinesisStream = new EmbeddedKinesisStream(TestUtils.noneToBlank("AKIAWIRSZAC4JFB22YG5"),
                TestUtils.noneToBlank("c9BjDlTacT53hDEtphTOS+lMaLSUXrcu/bUwGBGO"));
        System.out.println("Connection success...");

    }

    public void stop()
    {
        embeddedKinesisStream.close();
    }

    public static QueryRunner getQueryRunner() throws IOException {
        try {
            KinesisPlugin sheetsPlugin = new KinesisPlugin();
            queryRunner = LocalQueryRunner.builder(SESSION).build();

            queryRunner.installPlugin(sheetsPlugin);
            Map<String, String> kinesisConfig = ImmutableMap.of(
                    "kinesis.default-schema", "default",
                    "kinesis.access-key", "",
                    "kinesis.secret-key", "",
                    "kinesis.table-description-location", "src/test/resources/tableDescriptions");
            queryRunner.createCatalog("kinesis", "kinesis", kinesisConfig);

            /*queryRunner.createCatalog(GOOGLE_SHEETS, GOOGLE_SHEETS,
                    getConfigProperties(getCredentialsAsString()));*/

            System.out.println(" STEP 1: queryRunner object created");

        } catch (Exception exception) {
            System.out.println("Util=GoogleSheetsUtils|Method=getQueryRunner|message=Error occurred " + exception.getMessage());
            throw new IOException(exception);
        }

        return queryRunner;
    }

    /**
     *
     * @throws Exception
     */
    public void spinUp( String streamName)
            throws Exception
    {
        /*System.out.println( "Creating KINESIS stream : " + streamName);
        //embeddedKinesisStream.createStream(2, streamName);
        System.out.println( "Creating KINESIS stream Done : " + streamName);*/
        //this.queryRunner = new StandaloneQueryRunner(SESSION);
        System.out.println("SpinUp started...");
        //KinesisPlugin sheetsPlugin = new KinesisPlugin();
        System.out.println("KinesisPlugin created....");
        queryRunner = LocalQueryRunner.builder(SESSION).build();

        //queryRunner.installPlugin(sheetsPlugin);
        System.out.println("Query Runner created..."+queryRunner);
        /*this.queryRunner = LocalQueryRunner.builder(SESSION).build();*/
        Path tempDir = Files.createTempDirectory("tempdir");

        File baseFile = new File("C:\\Trino_project\\trino-master\\plugin\\trino-kinesis\\src\\test\\resources\\tableDescriptions\\CustomTable.json");
        File file = new File(tempDir.toAbsolutePath().toString() + "/" + streamName + ".json");

        try (Stream<String> lines = Files.lines(baseFile.toPath())) {
            List<String> replaced = lines
                    .map(line -> line.replaceAll("TABLE_NAME", streamName))
                    .map(line -> line.replaceAll("STREAM_NAME", streamName))
                    .collect(Collectors.toList());
            Files.write(file.toPath(), replaced);
            System.out.println("stream name and value passed to the stream");
        }
        System.out.println("starting installKinesisPlugin task....");
        TestUtils.installKinesisPlugin(queryRunner, tempDir.toAbsolutePath().toString(),
                TestUtils.noneToBlank("AKIAWIRSZAC4JFB22YG5"),
                TestUtils.noneToBlank("c9BjDlTacT53hDEtphTOS+lMaLSUXrcu/bUwGBGO"));

        System.out.println( "QueryRunner created Successfully" + queryRunner.toString());

    }


    private void createJsonMessages(String streamName, int idStart, boolean compress)
    {
        List<String> list = Arrays.asList("AppFlow", "AFQ", "Quicksight" , "Athena Engine" , "Trino");
        String jsonFormat = "{\"id\" : %d, \"name\" : \"%s\"}";
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        System.out.println("Insert JSON value into kinesis stream");
        for (int i = 0; i < list.size(); i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            long id = idStart + i;
            String name = list.get( i );
            String jsonVal = format(jsonFormat, id, name);

            System.out.println( jsonVal);

            // ? with StandardCharsets.UTF_8
            if (compress) {
                putRecordsRequestEntry.setData(ByteBuffer.wrap(jsonVal.getBytes(UTF_8)));
            }
            else {
                putRecordsRequestEntry.setData(ByteBuffer.wrap(jsonVal.getBytes(UTF_8)));
            }
            putRecordsRequestEntry.setPartitionKey(Long.toString(id));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        embeddedKinesisStream.getKinesisClient().putRecords(putRecordsRequest);
    }
    public void tearDown(String streamName)
    {
        embeddedKinesisStream.deleteStream(streamName);
        queryRunner.close();
    }


    public  List<String> streamNames(){
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(20);
        ListStreamsResult listStreamsResult = amazonKinesisClient.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();
        while (listStreamsResult.getHasMoreStreams())
        {
            if (streamNames.size() > 0) {
                listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
            }
            listStreamsResult = amazonKinesisClient.listStreams(listStreamsRequest);
            streamNames.addAll(listStreamsResult.getStreamNames());
        }
        return streamNames;
    }

}
