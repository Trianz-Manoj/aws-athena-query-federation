/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.athena.connectors.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import com.trianz.athena.connectors.kinesis.util.EmbeddedKinesisStream;

import io.trino.plugin.kinesis.KinesisPlugin;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
//import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

/**
 * Note: this is an integration test that connects to AWS Kinesis.
 * <p>
 * Only run if you have an account setup where you can create streams and put/get records.
 * You may incur AWS charges if you run this test.  You probably want to setup an IAM
 * user for your CI server to use.
 */
@Test(singleThreaded = true)
public class KinesisMinimumFunctionalityStandAlone
{
    private EmbeddedKinesisStream embeddedKinesisStream;
    private static AmazonKinesisClient amazonKinesisClient;
    static QueryRunner queryRunner;
    //private StandaloneQueryRunner queryRunner;

    @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })


    public static void main(String[] args) throws Exception {
        String streamName = "firststream";
        KinesisMinimumFunctionalityStandAlone testMinimalFunctionalityCustomize = new KinesisMinimumFunctionalityStandAlone();
        testMinimalFunctionalityCustomize.start();
        testMinimalFunctionalityCustomize.spinUp( streamName);
        //System.out.println(testMinimalFunctionalityCustomize.streamNames());

        testMinimalFunctionalityCustomize.runQuery(streamName);
        //System.out.println(testMinimalFunctionalityCustomize.streamNames());
        System.out.println("Tearing down  Environment .... Started");
        //testMinimalFunctionalityCustomize.tearDown( streamName);
        System.out.println("Tearing down  Environment .... Done");

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
        embeddedKinesisStream = new EmbeddedKinesisStream((""),
                (""));
        //embeddedKinesisStream = new EmbeddedKinesisStream(secretKey,secretKey);

    }

    public void stop()
    {
        embeddedKinesisStream.close();
    }

    /**
     *
     * @throws Exception
     */
    public void spinUp( String streamName)
            throws Exception
    {
        //System.out.println( "Creating KINESIS stream : " + streamName);
        //embeddedKinesisStream.createStream(2, streamName);
        //System.out.println( "Creating KINESIS stream Done : " + streamName);

       KinesisPlugin kinesisPlugin = new KinesisPlugin();
        queryRunner = LocalQueryRunner.builder(createSession()).build();
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

       // TestUtils.installKinesisPlugin(queryRunner, tempDir.toAbsolutePath().toString(),
                //TestUtils.noneToBlank(""),
               // TestUtils.noneToBlank(""));

        System.out.println( "QueryRunner created Successfully" + queryRunner.toString());

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
    static Session createSession() {
        return Session.builder(new SessionPropertyManager())
                .setIdentity(Identity.ofUser("user"))
                .setSource("source")
                .setCatalog("kinesis")
                .setSchema("default")
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setQueryId(new QueryId("dummy"))
                .build();
    }
    /*public static void installKinesisPlugin(QueryRunner queryRunner, String tableDescriptionLocation, String accessKey, String secretKey)
    {
       *//* KinesisPlugin kinesisPlugin = new KinesisPlugin();
        queryRunner.installPlugin(kinesisPlugin);*//*

        Map<String, String> kinesisConfig = ImmutableMap.of(
                "kinesis.default-schema", "default",
                "kinesis.access-key", accessKey,
                "kinesis.secret-key", secretKey,
                "kinesis.table-description-location", tableDescriptionLocation);

        queryRunner.createCatalog("kinesis", "kinesis", kinesisConfig);
    }*/

}


