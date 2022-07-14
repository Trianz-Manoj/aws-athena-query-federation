package com.trianz.athena.connectors.kinesis.util;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.StreamDescription;

import java.io.Closeable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EmbeddedKinesisStream implements Closeable  {
    private AmazonKinesisClient amazonKinesisClient;

    public EmbeddedKinesisStream(String accessKey, String secretKey)
    {
        this.amazonKinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(accessKey, secretKey));
    }

    @Override
    public void close() {}

    private String checkStreamStatus(String streamName)
    {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);

        StreamDescription streamDescription = amazonKinesisClient.describeStream(describeStreamRequest).getStreamDescription();
        return streamDescription.getStreamStatus();
    }

    public void createStream(int shardCount, String streamName)
    {
        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        createStreamRequest.setStreamName(streamName);
        createStreamRequest.setShardCount(shardCount);

        amazonKinesisClient.createStream(createStreamRequest);
        try {
            while (!checkStreamStatus(streamName).equals("ACTIVE")) {
                MILLISECONDS.sleep(1000);
            }
        }
        catch (Exception e) {
        }
    }

    public AmazonKinesisClient getKinesisClient()
    {
        return amazonKinesisClient;
    }

    public void deleteStream(String streamName)
    {
        DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
        deleteStreamRequest.setStreamName(streamName);
        amazonKinesisClient.deleteStream(deleteStreamRequest);
    }
}
