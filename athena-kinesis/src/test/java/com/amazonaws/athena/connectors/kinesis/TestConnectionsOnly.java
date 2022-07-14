package com.amazonaws.athena.connectors.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.trianz.athena.connectors.kinesis.util.EmbeddedKinesisStream;
import org.testng.annotations.Parameters;

public class TestConnectionsOnly {
    private EmbeddedKinesisStream embeddedKinesisStream;
    private static AmazonKinesisClient amazonKinesisClient;

    @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    public static void main(String[] args) {
        TestConnectionsOnly testConnectionsOnly = new TestConnectionsOnly();
        testConnectionsOnly.start("kinesis.awsAccessKey",
                "kinesis.awsSecretKey");
    }

    public void start(String accessKey, String secretKey)
    {
        /*embeddedKinesisStream = new EmbeddedKinesisStream((""),
                "));*/
        embeddedKinesisStream = new EmbeddedKinesisStream(accessKey,secretKey);

    }
}
